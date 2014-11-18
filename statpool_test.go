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
	EZKey = "finchbasket"
	Host  = ":8000"
)

var reqs = make(chan []byte, 1)

func init() {

	// set up test listener
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		// pass body bytes since req.Body auto-closes on handler end
		data, _ := ioutil.ReadAll(req.Body)
		reqs <- data
		json.NewEncoder(w).Encode(&statResponse{Status: http.StatusOK})
	})
	go func() { log.Fatal(http.ListenAndServe(Host, nil)) }()
}

func TestStatPool(t *testing.T) {

	stats := NewPool("http://"+Host, EZKey, 100*time.Millisecond)
	stats.SetDevLogger(log.New(os.Stderr, "statpool: ", log.LstdFlags))
	stats.SetPrefix("prefix:")

	stats.Count("darts", 1)
	stats.Count("darts", 1)
	stats.Count("darts", 1)
	stats.Count("darts", 4)
	stats.Value("players", 2, time.Now())
	stats.Duration("quickest time", time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	stats.Stop()

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
		case "prefix:darts":
			if stat.Count != 7 {
				t.Errorf("Expected: 7, got: %d", stat.Count)
			}
			if stat.Timestamp == 0 {
				t.Errorf("Did not get a valid timestamp")
			}
		case "prefix:players":
			if stat.Value != 2 {
				t.Errorf("Expected: 2, got: %d", stat.Value)
			}
			if stat.Timestamp == 0 {
				t.Errorf("Did not get a valid timestamp")
			}
		case "prefix:quickest time":
			if stat.Value != 1000 {
				t.Errorf("Expected: 1000, got: %d", stat.Value)
			}
		}
	}

}

func TestNilPool(t *testing.T) {

	stat := NewNilPool()
	stat.Count("key", 1)
	stat.Value("key", 1, time.Now())
	stat.Duration("key", time.Second)

}
