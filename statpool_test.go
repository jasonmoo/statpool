package statpool

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"testing"
	"time"
)

type (
	Payload struct {
		EZKey string `json:"ezkey"`
		Data  []struct {
			Key       string  `json:"stat"`
			Value     float64 `json:"value"`
			Count     float64 `json:"count"`
			Timestamp int64   `json:"t"`
		} `json:"data"`
	}
)

const (
	EZKey = "yo mam"
	Host  = ":8000"
)

var reqs = make(chan []byte, 1)

func init() {

	// set up test listener
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		// pass body bytes since req.Body auto-closes on handler end
		data, _ := ioutil.ReadAll(req.Body)
		reqs <- data
	})
	go func() { log.Fatal(http.ListenAndServe(Host, nil)) }()
}

func TestStatPool(t *testing.T) {

	pool := NewPool("http://"+Host, EZKey, 100*time.Millisecond)
	pool.SetDevLogger(log.New(os.Stderr, "statpool: ", log.LstdFlags))

	pool.Count("darts", 1)
	pool.Count("darts", 1)
	pool.Count("darts", 1)
	pool.Count("darts", 4)
	pool.Value("players", 2, time.Now())
	pool.Duration("quickest time", time.Millisecond, time.Now())

	time.Sleep(200 * time.Millisecond)
	pool.Stop()

	var p Payload

	if err := json.Unmarshal(<-reqs, &p); err != nil {
		t.Error(err)
	}

	if p.EZKey != EZKey {
		t.Errorf("Expected: %q, got: %q", EZKey, p.EZKey)
	}

	if len(p.Data) != 3 {
		t.Errorf("Expected: 3 stats, got: %d", len(p.Data))
	}

	for _, stat := range p.Data {
		switch stat.Key {
		case "darts":
			if stat.Count != 7 {
				t.Errorf("Expected: 7, got: %d", stat.Count)
			}
			if stat.Timestamp == 0 {
				t.Errorf("Did not get a valid timestamp")
			}
		case "players":
			if stat.Value != 2 {
				t.Errorf("Expected: 2, got: %d", stat.Value)
			}
			if stat.Timestamp == 0 {
				t.Errorf("Did not get a valid timestamp")
			}
		case "quickest time":
			if stat.Value != 1000 {
				t.Errorf("Expected: 1000, got: %d", stat.Value)
			}
			if stat.Timestamp == 0 {
				t.Errorf("Did not get a valid timestamp")
			}
		}
	}

}
