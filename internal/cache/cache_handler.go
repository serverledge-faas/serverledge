package cache

import (
	"sync"
	"time"
)

var (
	Instance *Cache
)

var lock = &sync.Mutex{}
var (
	DefaultExp      time.Duration = 0 // default expiration
	CleanupInterval time.Duration = 0 //cleanup interval
	Size                          = 0
	Persist                       = false // default value used to test progress/partial datas. If false, they are only saved in local cache
)

// GetCacheInstance : singleton implementation to retrieve THE cache
func GetCacheInstance() *Cache {
	lock.Lock()
	defer lock.Unlock()

	if Instance == nil {

		Instance = New(DefaultExp, CleanupInterval, Size) // <-- thread safe
	}

	return Instance
}
