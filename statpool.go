package statpool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
		StatKey   string  `json:"stat"`
		Value     float64 `json:"value"`
		Timestamp int64   `json:"t,omitempty"`
	}

	CountStat struct {
		StatKey   string  `json:"stat"`
		Count     float64 `json:"count"`
		Timestamp int64   `json:"t,omitempty"`
	}
)

func NewPool(url, ezKey string, flushInterval time.Duration) *Pool {

	p := &Pool{
		ezKey: ezKey,
		url:   url,

		client: &http.Client{},
		Log:    log.New(ioutil.Discard, "", 0),

		stop:  make(chan struct{}),
		flush: make(chan struct{}),
		count: make(chan *CountStat, 512),
		value: make(chan *ValueStat, 512),
	}

	go func() {
		tick := time.NewTicker(flushInterval)
		for {
			select {
			case <-stop:
				p.Log.Println("exiting")
				return
			case <-flush:
				if err := p.flush(); err != nil {
					p.Log.Println(err)
				}
			case <-tick:
				if err := p.flush(); err != nil {
					p.Log.Println(err)
				}
			case v := <-count:
				if stat, exists := p.counts[v.StatKey]; exists {
					stat.Count += v.Count
				} else {
					p.counts[v.StatKey] = v
					p.values = append(p.values, v)
				}
			case v := <-value:
				p.values = append(p.values, v)
			}
		}
	}()

	return p
}

func (p *Pool) Verbose() {
	p.Log = log.New(os.Stderr, "statpool: ", p.Log.LstdFlags)
}

func (p *Pool) flush() error {

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
		p.Log.Println("unprocessed aggregate:", buf.String())
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.Log.Println("unprocessed aggregate:", buf.String())
		return fmt.Errorf("Received http status code: %d from stathat", resp.StatusCode)
	}

	return nil

}

func (p *Pool) Count(key string, val float64) {
	stat := &CountStat{
		StatKey: key,
		Count:   val,
	}
	select {
	case p.count <- stat:
	default:
		p.Log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Value(key string, val float64, timestamp time.Time) {
	stat := &ValueStat{
		StatKey: key,
		Value:   val,
	}
	select {
	case p.value <- stat:
	default:
		p.Log.Printf("channels backed up, dropping stat: %+v", stat)
	}
}

func (p *Pool) Stop() {
	p.stop <- struct{}{}
}
