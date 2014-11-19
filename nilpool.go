package statpool

import "time"

type NilPool struct{}

func NewNilPool() NilPool {
	return NilPool{}
}

func (_ NilPool) Count(_ string, _ float64)              {}
func (_ NilPool) Value(_ string, _ float64, _ time.Time) {}
func (_ NilPool) Duration(_ string, _ time.Duration)     {}
