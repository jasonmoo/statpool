package statpool

import (
	"log"
	"math/rand"
	"time"
)

type LoggerPool struct {
	l *log.Logger
}

func NewLoggerPool(logger *log.Logger) *LoggerPool {
	return &LoggerPool{l: logger}
}

func (l *LoggerPool) Count(key string, val float64) {
	l.l.Printf("%s:%g", key, val)
}

func (l *LoggerPool) Value(key string, val float64, _ time.Time) {
	l.l.Printf("%s:%g", key, val)
}

func (l *LoggerPool) Duration(key string, val time.Duration) {
	l.l.Printf("%s:%s", key, val)
}

func (l *LoggerPool) SampledDuration(key string, val time.Duration, rate float64) {
	if rate < rand.Float64() {
		l.l.Printf("%s:%s", key, val)
	}
}
