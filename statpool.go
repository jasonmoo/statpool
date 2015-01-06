package statpool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

type (
	Stater interface {
		Count(key string, val float64)
		Value(key string, val float64, timestamp time.Time)
		Duration(key string, val time.Duration)
	}

	Pool struct {
		// api key
		ezKey  string
		url    string
		client *http.Client
		log    *log.Logger

		// output stats to
		devlogger *log.Logger

		// communication
		stop     chan struct{}
		done     chan struct{}
		flush    chan struct{}
		flushing sync.WaitGroup
		count    chan *CountStat
		value    chan *ValueStat

		// prefix all keys with
		prefix string
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

	statPayload struct {
		EZKey string        `json:"ezkey"`
		Data  []interface{} `json:"data"`
	}
	statResponse struct {
		Status  int    `json:"status"`
		Message string `json:"msg"`
	}
)

const (
	DefaultStathatEndpoint = "https://api.stathat.com/ez"
	chunkSize              = 3000
)

func NewPool(url, ezKey string, flushInterval time.Duration) *Pool {

	p := &Pool{
		ezKey: ezKey,
		url:   url + "?ezkey=" + ezKey,

		client: &http.Client{},
		log:    log.New(os.Stderr, "statpool: ", log.LstdFlags),

		flush:    make(chan struct{}),
		flushing: sync.WaitGroup{},
		stop:     make(chan struct{}),

		count: make(chan *CountStat, 512),
		value: make(chan *ValueStat, 512),
	}

	go func() {

		var (
			values = []interface{}{}
			counts = map[string]*CountStat{}
			tick   = time.NewTicker(flushInterval)

			rotate_values = func() []interface{} {
				stats := values
				values = []interface{}{}
				counts = map[string]*CountStat{}
				return stats
			}

			doflush = func(stats []interface{}) {
				if err := p.doflush(stats); err != nil {
					p.log.Println(err)
				}
				p.flushing.Done()
			}
		)

		for {
			select {
			case v := <-p.count:
				if stat, exists := counts[v.Key]; exists {
					stat.Count += v.Count
				} else {
					counts[v.Key] = v
					values = append(values, v)
				}

			case v := <-p.value:
				values = append(values, v)

			case <-tick.C:
				p.flushing.Add(1) // add one so ending done call doesn't panic
				go doflush(rotate_values())

			case <-p.stop:
				tick.Stop()
				doflush(rotate_values())
				return

			case <-p.flush:
				doflush(rotate_values())
			}
		}
	}()

	return p
}

func (p *Pool) SendCount(stat *CountStat) {
	select {
	case p.count <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) SendValue(stat *ValueStat) {
	select {
	case p.value <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Count(key string, val float64) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%g", p.prefix, key, val)
	}
	p.SendCount(&CountStat{Key: p.prefix + key, Count: val})
}

func (p *Pool) Value(key string, val float64, timestamp time.Time) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%g", p.prefix, key, val)
	}
	p.SendValue(&ValueStat{Key: p.prefix + key, Value: val, Timestamp: timestamp.Unix()})
}

func (p *Pool) Duration(key string, val time.Duration) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%s", p.prefix, key, val)
	}
	p.SendValue(&ValueStat{Key: p.prefix + key, Value: float64(val) / float64(time.Millisecond)})
}

func (p *Pool) SampledDuration(key string, val time.Duration, rate float64) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%s", p.prefix, key, val)
	}
	if rate < rand.Float64() {
		p.SendValue(&ValueStat{Key: p.prefix + key, Value: float64(val) / float64(time.Millisecond)})
	}
}

func (p *Pool) SetPrefix(prefix string) {
	p.prefix = prefix
}

func (p *Pool) SetDevLogger(l *log.Logger) {
	p.devlogger = l
}

func (p *Pool) Stop() {
	p.flushing.Add(1)
	p.stop <- struct{}{}
	p.flushing.Wait()
}

func (p *Pool) Flush() {
	p.flushing.Add(1)
	p.flush <- struct{}{}
	p.flushing.Wait()
}

func (p *Pool) doflush(values []interface{}) error {

	var start time.Time
	if p.devlogger != nil {
		p.devlogger.Println("doflush")
		start = time.Now()
		defer func() { p.devlogger.Printf("flush completed in %s", time.Since(start)) }()
	}

	// if no work just return
	if len(values) == 0 {
		return nil
	}

	// set the flush time as the aggregated count time
	now := time.Now().Unix()
	for _, val := range values {
		if count, ok := val.(*CountStat); ok {
			count.Timestamp = now
		}
	}

	// chunk the sends to ensure data size is not excessive
	var chunks [][]interface{}
	for len(values) > chunkSize {
		chunks = append(chunks, values[:chunkSize])
		values = values[chunkSize:]
	}
	chunks = append(chunks, values)

	errs := make(chan error, len(chunks))

	for _, chunk := range chunks {
		go p.send(chunk, errs)
	}

	// toss back the first error for now... :/
	for i := 0; i < len(chunks); i++ {
		if err := <-errs; err != nil {
			return err
		}
	}

	return nil

}

func (p *Pool) send(chunk []interface{}, errs chan error) {

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&statPayload{
		EZKey: p.ezKey,
		Data:  chunk,
	}); err != nil {
		errs <- err
	}

	req, err := http.NewRequest("POST", p.url, buf)
	if err != nil {
		errs <- err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Println("unprocessed aggregate:", buf.String())
		errs <- err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.Println("unprocessed aggregate:", buf.String())
		errs <- fmt.Errorf("Received http status code: %d", resp.StatusCode)
	}

	var sresp statResponse
	if err := json.NewDecoder(resp.Body).Decode(&sresp); err != nil {
		errs <- err
	}

	if sresp.Status != http.StatusOK {
		errs <- fmt.Errorf("%d : %s", sresp.Status, sresp.Message)
	}

	errs <- nil

}
