//go:build !windows

package main

import "syscall"

var (
	errRefused         error = syscall.ECONNREFUSED
	errNetUnreachable  error = syscall.ENETUNREACH
	errHostUnreachable error = syscall.EHOSTUNREACH
)
