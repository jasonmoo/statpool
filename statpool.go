package statpool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"time"
)

type (
	Pool struct {
		// api key
		ezKey  string
		url    string
		client *http.Client
		log    *log.Logger

		// output stats to
		devlogger *log.Logger

		// communication
		stop  chan struct{}
		flush chan struct{}
		count chan *CountStat
		value chan *ValueStat

		// counts for aggregation
		counts map[string]*CountStat

		// all values for output
		values []interface{}
	}

	ValueStat struct {
		Key       string  `json:"stat"`
		Value     float64 `json:"value"`
		Timestamp int64   `json:"t,omitempty"`
	}

	CountStat struct {
		Key       string  `json:"stat"`
		Count     float64 `json:"count"`
		Timestamp int64   `json:"t,omitempty"`
	}
)

func NewPool(url, ezKey string, flushInterval time.Duration) *Pool {

	p := &Pool{
		ezKey: ezKey,
		url:   url,

		client: &http.Client{},
		log:    log.New(os.Stderr, "statpool: ", log.LstdFlags|log.Lshortfile),

		stop:  make(chan struct{}),
		flush: make(chan struct{}),
		count: make(chan *CountStat, 512),
		value: make(chan *ValueStat, 512),

		counts: map[string]*CountStat{},
		values: []interface{}{},
	}

	go func() {
		tick := time.NewTicker(flushInterval)
		for {
			select {
			case <-p.stop:
				p.log.Println("exiting")
				tick.Stop()
				return
			case <-p.flush:
				if p.devlogger != nil {
					p.devlogger.Println("flushing")
				}
				if err := p.doflush(); err != nil {
					p.log.Println(err)
				}
			case <-tick.C:
				if p.devlogger != nil {
					p.devlogger.Println("tick")
				}
				if err := p.doflush(); err != nil {
					p.log.Println(err)
				}
			case v := <-p.count:
				if stat, exists := p.counts[v.Key]; exists {
					stat.Count += v.Count
				} else {
					p.counts[v.Key] = v
					p.values = append(p.values, v)
				}
			case v := <-p.value:
				p.values = append(p.values, v)
			}
		}
	}()

	return p
}

func (p *Pool) doflush() error {

	if p.devlogger != nil {
		p.devlogger.Println("doflush")
	}

	// if no work just return
	if len(p.counts) == 0 && len(p.values) == 0 {
		return nil
	}

	// set the flush time as the aggregated count time
	now := time.Now().Unix()
	for _, count := range p.counts {
		count.Timestamp = now
	}

	// grab values and reset
	values := p.values
	p.values = []interface{}{}
	p.counts = map[string]*CountStat{}

	type payload struct {
		EZKey string        `json:"ezkey"`
		Data  []interface{} `json:"data"`
	}

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&payload{
		EZKey: p.ezKey,
		Data:  values,
	}); err != nil {
		return err
	}

	req, err := http.NewRequest("POST", p.url, buf)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json, charset=UTF-8")

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Println("unprocessed aggregate:", buf.String())
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.Println("unprocessed aggregate:", buf.String())
		return fmt.Errorf("Received http status code: %d from stathat", resp.StatusCode)
	}

	return nil

}

func (p *Pool) Count(key string, val float64) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s:%g", key, val)
	}
	stat := &CountStat{
		Key:   key,
		Count: val,
	}
	select {
	case p.count <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Value(key string, val float64, timestamp time.Time) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s:%g", key, val)
	}
	stat := &ValueStat{
		Key:       key,
		Value:     val,
		Timestamp: timestamp.Unix(),
	}
	select {
	case p.value <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Duration(key string, val time.Duration, timestamp time.Time) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s:%s", key, val)
	}
	stat := &ValueStat{
		Key:       key,
		Value:     float64(val.Nanoseconds()) / 1000, // track milliseconds
		Timestamp: timestamp.Unix(),
	}
	select {
	case p.value <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) SetDevLogger(l *log.Logger) {
	p.devlogger = l
}

func (p *Pool) Stop() {
	p.stop <- struct{}{}
}

func (p *Pool) Flush() {
	p.flush <- struct{}{}
}
