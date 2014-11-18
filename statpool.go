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
		stop  chan struct{}
		done  chan struct{}
		flush chan struct{}
		count chan *CountStat
		value chan *ValueStat

		// prefix all keys with
		prefix string

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

		stop:  make(chan struct{}),
		done:  make(chan struct{}),
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
				if err := p.doflush(); err != nil {
					p.log.Println(err)
				}
				p.done <- struct{}{}
				return
			case <-p.flush:
				if p.devlogger != nil {
					p.devlogger.Println("flushing")
				}
				if err := p.doflush(); err != nil {
					p.log.Println(err)
				}
				p.done <- struct{}{}
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

func (p *Pool) SetPrefix(prefix string) {
	p.prefix = prefix
}

func (p *Pool) SetDevLogger(l *log.Logger) {
	p.devlogger = l
}

func (p *Pool) Stop() {
	p.stop <- struct{}{}
	<-p.done
}

func (p *Pool) Flush() {
	p.flush <- struct{}{}
	<-p.done
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

	// chunk the sends to ensure data size is not excessive
	var chunks [][]interface{}
	for i := 0; i+chunkSize < len(values); i += chunkSize {
		chunks = append(chunks, values[:i+chunkSize])
		values = values[i+chunkSize:]
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

func (p *Pool) Count(key string, val float64) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%g", p.prefix, key, val)
	}
	stat := &CountStat{
		Key:   p.prefix + key,
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
		p.devlogger.Printf("%s%s:%g", p.prefix, key, val)
	}
	stat := &ValueStat{
		Key:       p.prefix + key,
		Value:     val,
		Timestamp: timestamp.Unix(),
	}
	select {
	case p.value <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Duration(key string, val time.Duration) {
	if p.devlogger != nil {
		p.devlogger.Printf("%s%s:%s", p.prefix, key, val)
	}
	stat := &ValueStat{
		Key:   p.prefix + key,
		Value: float64(val.Nanoseconds()) / 1000, // track milliseconds
	}
	select {
	case p.value <- stat:
	default:
		p.log.Printf("channels backed up, dropping stat: %+v", stat)
	}
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
