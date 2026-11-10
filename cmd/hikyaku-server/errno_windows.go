package main

import "golang.org/x/sys/windows"

// Winsock reports a failed connect with its own error numbers; syscall's
// ECONNREFUSED and friends are values Go invents on Windows, which no dial returns.
var (
	errRefused         error = windows.WSAECONNREFUSED
	errNetUnreachable  error = windows.WSAENETUNREACH
	errHostUnreachable error = windows.WSAEHOSTUNREACH
)
