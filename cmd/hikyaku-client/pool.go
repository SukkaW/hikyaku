package main

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SukkaW/hikyaku/proxy"
	"github.com/xtaci/smux"
)

const (
	dialTimeout  = 5 * time.Second
	minBackoff   = time.Second
	maxBackoff   = 5 * time.Second
	suspectAfter = 5 * time.Second
	healthCheck  = 500 * time.Millisecond
	carrierWait  = 10 * time.Second
	carrierPoll  = 50 * time.Millisecond
	slowStrikes  = 3
	slowEvery    = 2 * time.Second
	metricsEvery = 30 * time.Second
	probeWait    = 5 * time.Second
	// A ping or acknowledgement is overdue after TCP's retransmission timeout for the
	// carrier (RFC 6298: SRTT + 4·RTTVAR) but never under replyRoundTrips smoothed
	// round trips, kept within minReplyDeadline and maxReplyDeadline, and
	// maxReplyDeadline until a round trip is measured. The round-trip floor keeps GEO,
	// whose queues and loss recovery stretch replies well past RTTVAR, at the cap.
	minReplyDeadline = time.Second
	maxReplyDeadline = 3 * time.Second
	replyRoundTrips  = 5
	// Slowness is judged only from pongs to pings sent while a carrier carried no
	// data, so a busy carrier's own queue cannot make it look slow: the median of its
	// last quietSamples such round trips within quietWindow, needing minQuietSamples,
	// against the best other carrier's, with a margin of half the best or
	// minSlowMargin, whichever is larger.
	quietSamples    = 5
	minQuietSamples = 3
	quietWindow     = 10 * time.Second
	minSlowMargin   = 200 * time.Millisecond
	// Pings go out only when a carrier is quiet: often while it was in use recently,
	// which keeps the server's congestion window from shrinking during reading
	// pauses, and rarely once idle. A busy carrier still gets one every second, so its
	// expected wait stays current under load.
	quietAfter = 500 * time.Millisecond
	warmFor    = 30 * time.Second
	warmPing   = 500 * time.Millisecond
	coldPing   = 15 * time.Second
	busyPing   = time.Second
	// deadAfter closes a standby carrier whose ping has gone unanswered this long.
	deadAfter = 30 * time.Second
	// minWaitBand is the least a carrier's expected wait may exceed the best one's and
	// still count as tied; otherwise the band is a quarter of the best.
	minWaitBand = 20 * time.Millisecond
)

var errNoCarrier = errors.New("no carrier is ready")

type carrier struct {
	slot    int
	session *smux.Session
	conn    *proxy.Conn
	ready   time.Time
	latest  atomic.Int64 // the latest of those round trips in nanoseconds; 0 until the first

	lastStream atomic.Int64           // unix nanoseconds when a stream last closed
	goaway     atomic.Pointer[string] // the server's reason, once it sent GOAWAY
	pingNow    chan bool              // asks for a ping; true if the carrier is quiet

	mu         sync.Mutex
	unanswered map[*replayStream]struct{}
	waiting    map[*carrierStream]time.Time // streams still waiting for their acknowledgement
	pings      map[uint32]ping              // pings still waiting for their pong
	nextPing   uint32
	quiet      []sample      // round trips of pings sent while quiet, oldest first
	srtt       time.Duration // smoothed round trip from pongs and acknowledgements (RFC 6298); 0 until the first
	rttvar     time.Duration // its variation
}

type ping struct {
	sent  time.Time
	quiet bool // the carrier carried no data when it was sent
}

type sample struct {
	at  time.Time
	rtt time.Duration
}

// pool opens carriers on demand, up to a fixed number of slots; an opened slot is
// nil while its carrier is down and keeps being redialed. Silent carriers wait on
// standby until their slot has a replacement.
type pool struct {
	server   string
	psk      []byte
	mu       sync.Mutex
	carriers []*carrier
	standby  []*carrier
	opened   int
	changed  chan struct{} // closed and replaced whenever a carrier becomes usable
	probing  atomic.Bool
}

func newPool(server string, psk []byte, carriers int) *pool {
	return &pool{server: server, psk: psk, carriers: make([]*carrier, carriers), changed: make(chan struct{})}
}

// warm opens the first carrier before any request, so one that arrives a round trip
// later finds it connected. Requests that arrive sooner wait for it as before and
// still ride its first flight.
func (p *pool) warm() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened == 0 {
		p.grow()
	}
}

// grow starts maintaining one more slot; the caller holds p.mu.
func (p *pool) grow() {
	if p.opened < len(p.carriers) {
		go p.maintain(p.opened)
		p.opened++
	}
}

func (p *pool) maintain(slot int) {
	var backoff time.Duration
	for {
		c, err := p.dial()
		if err != nil {
			log.Printf("carrier %d: %v", slot, err)
			backoff = min(max(2*backoff, minBackoff), maxBackoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		c.slot = slot
		p.set(slot, c)
		log.Printf("carrier %d: ready", slot)
		v := p.watch(c)
		silent := v == silent || v == noAck
		switch {
		case v == goingAway:
			// The server keeps serving the carrier's streams, so they stay where they are.
			p.toStandby(c)
			log.Printf("carrier %d: %s after %s, standing by", slot, *c.goaway.Load(), c.lifetime())
			go p.retire(c)
		case v == slow:
			p.toStandby(c)
			c.moveUnanswered()
			log.Printf("carrier %d: slower than the best other carrier while quiet for %d checks after %s, standing by",
				slot, slowStrikes, c.lifetime())
			go p.retire(c)
		case silent && c.conn.Acked():
			p.toStandby(c)
			c.moveUnanswered()
			if v == noAck {
				log.Printf("carrier %d: acknowledgement overdue by %s after %s, standing by", slot, c.replyDeadline(), c.lifetime())
			} else {
				log.Printf("carrier %d: silent for %s after %s, standing by", slot, c.replyDeadline(), c.lifetime())
			}
			go p.retire(c)
		case silent:
			// Never acknowledged, so it has not proven it reaches the server; failing
			// its streams now lets apps retry instead of waiting on a dead standby.
			p.set(slot, nil)
			c.session.Close()
			log.Printf("carrier %d: no acknowledgement after %s, closing", slot, c.lifetime())
		default:
			p.set(slot, nil)
			log.Printf("carrier %d: closed after %s: %s", slot, c.lifetime(), c.closeReason())
		}
	}
}

func (p *pool) set(slot int, c *carrier) {
	p.mu.Lock()
	p.carriers[slot] = c
	if c != nil {
		p.wake()
	}
	p.mu.Unlock()
}

// wake releases requests waiting for a carrier; the caller holds p.mu.
func (p *pool) wake() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *pool) toStandby(c *carrier) {
	p.mu.Lock()
	p.carriers[c.slot] = nil
	p.standby = append(p.standby, c)
	p.mu.Unlock()
}

func (p *pool) replaced(slot int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.carriers[slot]
	return c != nil && c.conn.Acked()
}

func (p *pool) dial() (*carrier, error) {
	conn, err := net.DialTimeout("tcp", p.server, dialTimeout)
	if err != nil {
		return nil, err
	}
	pc, err := proxy.ClientHandshake(conn, p.psk)
	if err != nil {
		conn.Close()
		return nil, err
	}
	c := &carrier{conn: pc, pings: make(map[uint32]ping), pingNow: make(chan bool, 1)}
	pc.OnControl(c.control)
	session, err := smux.Client(pc, proxy.MuxConfig())
	if err != nil {
		conn.Close()
		return nil, err
	}
	c.session, c.ready = session, time.Now()
	go c.sendPings()
	return c, nil
}

func (c *carrier) lifetime() time.Duration {
	return time.Since(c.ready).Round(time.Second)
}

// closeReason explains why the session ended; one the pool closed itself reaches
// the reader as net.ErrClosed.
func (c *carrier) closeReason() string {
	if err := c.conn.ReadErr(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err.Error()
	}
	return "closed"
}

// control handles the server's control messages on the carrier's reading goroutine.
func (c *carrier) control(cmd byte, body []byte) {
	switch cmd {
	case proxy.CtrlPong:
		if len(body) != 4 {
			return
		}
		id := binary.BigEndian.Uint32(body)
		c.mu.Lock()
		pg, ok := c.pings[id]
		delete(c.pings, id)
		if ok && pg.quiet {
			c.quiet = append(c.quiet, sample{time.Now(), time.Since(pg.sent)})
			if len(c.quiet) > quietSamples {
				c.quiet = c.quiet[1:]
			}
		}
		c.mu.Unlock()
		if ok {
			c.sample(time.Since(pg.sent))
		}
	case proxy.CtrlGoAway:
		reason := "server going away"
		if len(body) >= 2 && len(body) >= 2+int(body[1]) {
			reason = string(body[2 : 2+int(body[1])])
		}
		c.goaway.Store(&reason)
	}
}

// sendPings writes the pings asked for on pingNow. It runs apart from the health
// checks, so a carrier whose writes are stuck cannot stall them: its ping is
// recorded before the write and simply goes overdue.
func (c *carrier) sendPings() {
	for {
		select {
		case quiet := <-c.pingNow:
			c.mu.Lock()
			id := c.nextPing
			c.nextPing++
			c.pings[id] = ping{sent: time.Now(), quiet: quiet}
			c.mu.Unlock()
			var body [4]byte
			binary.BigEndian.PutUint32(body[:], id)
			if c.conn.WriteControl(proxy.CtrlPing, body[:]) != nil {
				return
			}
		case <-c.session.CloseChan():
			return
		}
	}
}

func (c *carrier) ping(now time.Time) {
	quiet := now.Sub(c.conn.LastData()) >= quietAfter && now.Sub(c.conn.LastSent()) >= quietAfter
	select {
	case c.pingNow <- quiet:
	default:
	}
}

// quietMedian returns the median of c's recent quiet round trips, if it has enough.
func (c *carrier) quietMedian(now time.Time) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var recent []time.Duration
	for _, s := range c.quiet {
		if now.Sub(s.at) <= quietWindow {
			recent = append(recent, s.rtt)
		}
	}
	if len(recent) < minQuietSamples {
		return 0, false
	}
	slices.Sort(recent)
	return recent[len(recent)/2], true
}

// pingInterval picks how often to ping a carrier from how long ago its last smux
// data arrived, how long ago its last stream closed, and how many it has open.
func pingInterval(sinceData, sinceStream time.Duration, streams int) time.Duration {
	switch {
	case sinceData < quietAfter:
		return busyPing
	case streams > 0 || sinceStream < warmFor:
		return warmPing
	default:
		return coldPing
	}
}

// pongOverdue reports whether a ping has waited at least d for its pong.
func (c *carrier) pongOverdue(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pg := range c.pings {
		if time.Since(pg.sent) >= d {
			return true
		}
	}
	return false
}

func (c *carrier) ackOverdue() bool {
	d := c.replyDeadline()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, opened := range c.waiting {
		if time.Since(opened) >= d {
			return true
		}
	}
	return false
}

// replyDeadline is how long a ping or acknowledgement on c may take before c is
// suspect.
func (c *carrier) replyDeadline() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.srtt == 0 {
		return maxReplyDeadline
	}
	return min(max(c.srtt+4*c.rttvar, replyRoundTrips*c.srtt, minReplyDeadline), maxReplyDeadline)
}

type verdict int

const (
	closed verdict = iota
	silent
	noAck
	slow
	goingAway
)

// watch pings the carrier and reports why it should leave its slot: its session
// closed, the server is going away, a ping went unanswered, a stream's
// acknowledgement is overdue, or it stayed clearly slower than the best other carrier
// for slowStrikes checks in a row.
func (p *pool) watch(c *carrier) verdict {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	var strikes int
	var lastPing time.Time
	lastSlowCheck, lastLog := time.Now(), time.Now()
	for {
		select {
		case <-c.session.CloseChan():
			return closed
		case <-tick.C:
			now := time.Now()
			// smux reports a dead connection to its streams but leaves the session open.
			if c.conn.ReadErr() != nil {
				c.session.Close()
				return closed
			}
			if c.goaway.Load() != nil {
				return goingAway
			}
			sinceStream := now.Sub(time.Unix(0, c.lastStream.Load()))
			// Ticks are healthCheck apart, so half a tick of slack keeps a tick that lands
			// just short of the interval from pushing the ping to the next one, which would
			// turn the 0.5 s warm ping into one a second.
			interval := pingInterval(now.Sub(c.conn.LastData()), sinceStream, c.session.NumStreams())
			if now.Sub(lastPing) >= interval-healthCheck/2 {
				lastPing = now
				c.ping(now)
			}
			quiet := p.linkQuiet()
			if quiet {
				p.probeOutage()
			}
			// A link-wide outage looks the same on every carrier, so while it lasts only
			// acknowledged carriers go to standby, keeping their streams; new carriers
			// keep waiting for their acknowledgement and the other rules wait too.
			if c.conn.Acked() && c.pongOverdue(c.replyDeadline()) {
				return silent
			}
			if !c.conn.Acked() && !quiet && now.Sub(c.ready) >= suspectAfter {
				return silent
			}
			if quiet {
				strikes = 0
				continue
			}
			if c.ackOverdue() {
				return noAck
			}
			if now.Sub(lastSlowCheck) >= slowEvery {
				lastSlowCheck = now
				if p.slower(c) {
					strikes++
				} else {
					strikes = 0
				}
				if strikes >= slowStrikes {
					return slow
				}
			}
			if now.Sub(lastLog) >= metricsEvery {
				lastLog = now
				c.mu.Lock()
				srtt := c.srtt
				c.mu.Unlock()
				log.Printf("carrier %d: rtt %s, reply deadline %s, %d streams", c.slot, srtt.Round(time.Millisecond),
					c.replyDeadline().Round(time.Millisecond), c.session.NumStreams())
			}
		}
	}
}

// slower reports whether c's quiet round trips sit clearly above the best other
// acknowledged carrier's; a carrier without enough of them, such as a busy one, is
// not judged, and with no other carrier to compare against nothing is slow.
func (p *pool) slower(c *carrier) bool {
	now := time.Now()
	mine, ok := c.quietMedian(now)
	if !c.conn.Acked() || !ok {
		return false
	}
	p.mu.Lock()
	best := time.Duration(-1)
	for _, o := range p.carriers {
		if o == nil || o == c || !o.conn.Acked() || o.session.IsClosed() {
			continue
		}
		if m, ok := o.quietMedian(now); ok && (best < 0 || m < best) {
			best = m
		}
	}
	p.mu.Unlock()
	return best >= 0 && slowerThan(mine, best)
}

func slowerThan(mine, best time.Duration) bool {
	return mine-best >= max(best/2, minSlowMargin)
}

// linkQuiet reports a link-wide outage: some carriers have been acknowledged, yet
// every one of them has a ping unanswered past its reply deadline. A carrier that was
// never acknowledged has heard nothing real, so it counts neither way.
func (p *pool) linkQuiet() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := false
	for _, c := range slices.Concat(p.carriers, p.standby) {
		if c == nil || !c.conn.Acked() || c.session.IsClosed() {
			continue
		}
		if !c.pongOverdue(c.replyDeadline()) {
			return false
		}
		seen = true
	}
	return seen
}

// probeOutage dials one fresh carrier at a time while the link stays quiet.
// Carriers whose path state was lost may never hear back, so the first acknowledged
// probe is what ends the outage; it then serves from standby until slot 0 has an
// acknowledged carrier again.
func (p *pool) probeOutage() {
	if !p.probing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer p.probing.Store(false)
		log.Printf("link: no carrier answered a ping in time, probing")
		for p.linkQuiet() {
			c, err := p.dial()
			if err != nil {
				time.Sleep(minBackoff)
				continue
			}
			deadline := time.Now().Add(probeWait)
			for !c.conn.Acked() && !c.session.IsClosed() && time.Now().Before(deadline) {
				time.Sleep(carrierPoll)
			}
			if !c.conn.Acked() {
				c.session.Close()
				continue
			}
			p.mu.Lock()
			p.standby = append(p.standby, c)
			p.wake()
			p.mu.Unlock()
			log.Printf("link: probe acknowledged, outage over")
			go p.retire(c)
			return
		}
	}()
}

// retire keeps a standby carrier for its existing streams, which survive a short
// outage, and closes it once its slot is replaced and those streams are done. It
// keeps pinging, so a recovery or an outage still shows; if the server never
// returns, it closes once a ping has gone unanswered for deadAfter.
func (p *pool) retire(c *carrier) {
	tick := time.NewTicker(healthCheck)
	defer tick.Stop()
	defer func() {
		p.mu.Lock()
		p.standby = slices.DeleteFunc(p.standby, func(s *carrier) bool { return s == c })
		p.mu.Unlock()
	}()
	for {
		select {
		case <-c.session.CloseChan():
			log.Printf("carrier %d: standby closed after %s: %s", c.slot, c.lifetime(), c.closeReason())
			return
		case <-tick.C:
			if c.conn.ReadErr() != nil {
				c.session.Close()
				continue
			}
			if c.session.NumStreams() == 0 && p.replaced(c.slot) {
				c.session.Close()
				log.Printf("carrier %d: standby retired after %s", c.slot, c.lifetime())
				return
			}
			if c.pongOverdue(deadAfter) {
				c.session.Close()
				log.Printf("carrier %d: standby closed after %s: no pong for %s", c.slot, c.lifetime(), deadAfter)
				return
			}
			c.ping(time.Now())
		}
	}
}

// openStream waits for a usable carrier when none is ready yet, such as right
// after start or an outage, so the stream rides the new carrier's first flight.
func (p *pool) openStream() (*carrier, *carrierStream, error) {
	deadline := time.NewTimer(carrierWait)
	defer deadline.Stop()
	for {
		// Taken before pick, so a carrier that appears in between still wakes us.
		p.mu.Lock()
		changed := p.changed
		p.mu.Unlock()
		if c := p.pick(); c != nil {
			s, err := c.session.OpenStream()
			if err != nil {
				return nil, nil, err
			}
			return c, c.newStream(s), nil
		}
		select {
		case <-changed:
		// A standby carrier that starts receiving again raises no signal.
		case <-time.After(healthCheck):
		case <-deadline.C:
			return nil, nil, errNoCarrier
		}
	}
}

func (p *pool) pick() *carrier {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.opened == 0 {
		p.grow()
	}
	best := quickest(p.carriers)
	if best != nil && (best.session.NumStreams() > 0 || !p.spare(best)) {
		// Keep an idle carrier in reserve: open another in the background as soon as
		// this one takes a stream, so a request that arrives during a long download
		// gets a carrier of its own instead of queueing behind it.
		p.grow()
	}
	if best == nil {
		// A standby carrier takes new streams only once its pings are answered again;
		// that proves more than a new carrier still waiting for its acknowledgement. One
		// the server is closing never does.
		best = fewestStreams(p.standby, func(c *carrier) bool {
			return !c.pongOverdue(c.replyDeadline()) && c.goaway.Load() == nil
		})
	}
	if best == nil {
		best = fewestStreams(p.carriers, nil)
	}
	return best
}

// spare reports whether a slot other than c's has an idle carrier or is still
// opening one; the caller holds p.mu.
func (p *pool) spare(c *carrier) bool {
	for slot := range p.opened {
		if slot == c.slot {
			continue
		}
		if o := p.carriers[slot]; o == nil || !o.conn.Acked() || o.session.NumStreams() == 0 {
			return true
		}
	}
	return false
}

func fewestStreams(carriers []*carrier, eligible func(*carrier) bool) *carrier {
	var best *carrier
	for _, c := range carriers {
		if c == nil || c.session.IsClosed() || (eligible != nil && !eligible(c)) {
			continue
		}
		if best == nil || c.session.NumStreams() < best.session.NumStreams() {
			best = c
		}
	}
	return best
}

// quickest picks the acknowledged carrier where a new stream should hear back
// soonest.
func quickest(carriers []*carrier) *carrier {
	now := time.Now()
	var acked []*carrier
	var waits []time.Duration
	var streams []int
	for _, c := range carriers {
		if c == nil || c.session.IsClosed() || !c.conn.Acked() {
			continue
		}
		acked = append(acked, c)
		waits = append(waits, c.expectedWait(now))
		streams = append(streams, c.session.NumStreams())
	}
	if len(acked) == 0 {
		return nil
	}
	return acked[choose(waits, streams)]
}

// choose returns which carrier to use from each one's expected wait, negative when
// unknown, and stream count. Waits within a band of the best count as tied, and so
// do unknown ones; among the tied, the fewest streams wins.
func choose(waits []time.Duration, streams []int) int {
	fastest := time.Duration(-1)
	for _, w := range waits {
		if w >= 0 && (fastest < 0 || w < fastest) {
			fastest = w
		}
	}
	limit := fastest + max(fastest/4, minWaitBand)
	best := -1
	for i, w := range waits {
		if fastest >= 0 && w > limit {
			continue
		}
		if best < 0 || streams[i] < streams[best] {
			best = i
		}
	}
	return best
}

// expectedWait estimates how long a new stream's request would wait for its
// acknowledgement on c: the latest round trip, or longer while a ping or an
// acknowledgement has gone unanswered for longer than that, which shows a stall
// before any reply returns. It is negative while c has neither.
func (c *carrier) expectedWait(now time.Time) time.Duration {
	wait := time.Duration(-1)
	if latest := c.latest.Load(); latest != 0 {
		wait = time.Duration(latest)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pg := range c.pings {
		wait = max(wait, now.Sub(pg.sent))
	}
	for _, opened := range c.waiting {
		wait = max(wait, now.Sub(opened))
	}
	return wait
}

// carrierStream strips the server's acknowledgement and result from the front of a
// stream: the acknowledgement becomes a round-trip sample for its carrier, and a
// failed result ends the stream with the server's reason.
type carrierStream struct {
	*smux.Stream
	c         *carrier
	opened    time.Time
	requested atomic.Int64 // unix nanoseconds of the first write, which carries the request
	answered  bool         // only touched by the single reader
}

func (c *carrier) newStream(s *smux.Stream) *carrierStream {
	cs := &carrierStream{Stream: s, c: c, opened: time.Now()}
	c.mu.Lock()
	if c.waiting == nil {
		c.waiting = make(map[*carrierStream]time.Time)
	}
	c.waiting[cs] = cs.opened
	c.mu.Unlock()
	return cs
}

func (s *carrierStream) Write(b []byte) (int, error) {
	s.requested.CompareAndSwap(0, time.Now().UnixNano())
	return s.Stream.Write(b)
}

func (s *carrierStream) Read(b []byte) (int, error) {
	if !s.answered {
		if err := proxy.ReadAck(s.Stream); err != nil {
			return 0, err
		}
		s.c.acknowledged(s, time.Since(time.Unix(0, s.requested.Load())))
		if err := proxy.ReadResult(s.Stream); err != nil {
			return 0, err
		}
		s.answered = true
	}
	return s.Stream.Read(b)
}

func (s *carrierStream) Close() error {
	s.c.mu.Lock()
	delete(s.c.waiting, s)
	s.c.mu.Unlock()
	s.c.lastStream.Store(time.Now().UnixNano())
	return s.Stream.Close()
}

func (c *carrier) acknowledged(s *carrierStream, rtt time.Duration) {
	c.mu.Lock()
	delete(c.waiting, s)
	c.mu.Unlock()
	c.sample(rtt)
}

func (c *carrier) sample(rtt time.Duration) {
	c.latest.Store(int64(rtt))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.srtt == 0 {
		c.srtt, c.rttvar = rtt, rtt/2
		return
	}
	diff := c.srtt - rtt
	if diff < 0 {
		diff = -diff
	}
	c.rttvar += (diff - c.rttvar) / 4
	c.srtt += (rtt - c.srtt) / 8
}
