package xlru

import "log/slog"

type slogAdapter struct {
	logger *slog.Logger
}

// NewSlogAdapter wraps a slog logger with the local Logger interface.
func NewSlogAdapter(logger *slog.Logger) Logger {
	if logger == nil {
		return nil
	}
	return &slogAdapter{logger: logger}
}

func (s *slogAdapter) Error(msg string, args ...any) {
	s.logger.Error(msg, args...)
}
