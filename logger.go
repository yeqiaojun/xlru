package xlru

// Logger is the minimal logging surface used by xlru.
type Logger interface {
	Error(msg string, args ...any)
}
