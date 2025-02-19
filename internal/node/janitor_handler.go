package node

import (
	"github.com/serverledge-faas/serverledge/internal/config"
	"sync"
	"time"
)

type janitor struct {
	Interval time.Duration
	stop     chan bool
}

var (
	Instance *janitor
)

var lock = &sync.Mutex{}

// GetJanitorInstance : singleton implementation to retrieve THE container janitor
func GetJanitorInstance() *janitor {
	lock.Lock()
	defer lock.Unlock()

	if Instance == nil {
		// todo adjust default interval
		Instance = runJanitor(time.Duration(config.GetInt(config.POOL_CLEANUP_PERIOD, 30)) * time.Second) // <-- thread safe
	}

	return Instance
}

func (j *janitor) run() {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			DeleteExpiredContainer()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func StopJanitor() {
	Instance.stop <- true
}

func runJanitor(ci time.Duration) *janitor {
	j := &janitor{
		Interval: ci,
		stop:     make(chan bool),
	}
	go j.run()
	return j
}
