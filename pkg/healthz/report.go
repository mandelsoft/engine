package healthz

import (
	"fmt"
	"sync"
	"time"

	"github.com/mandelsoft/logging"
)

var REALM = logging.DefineRealm("healthz", "Server health monitoring")

var log = logging.DynamicLogger(logging.DefaultContext(), REALM)

func Tick(key string) {
	lock.Lock()
	defer lock.Unlock()

	setCheck(key)
}

func Start(key string, period time.Duration) {
	lock.Lock()
	defer lock.Unlock()

	checks[key] = &check{time.Now(), 3 * period}
}

func End(key string) {
	lock.Lock()
	defer lock.Unlock()

	removeCheck(key)
}

type check struct {
	last    time.Time
	timeout time.Duration
}

var (
	checks = map[string]*check{}
	lock   sync.Mutex
)

func setCheck(key string) {
	c := checks[key]
	if c == nil {
		panic(fmt.Sprintf("check with key %q not configured", key))
	}
	c.last = time.Now()
}

func removeCheck(key string) {
	delete(checks, key)
}

func IsHealthy() bool {
	lock.Lock()
	defer lock.Unlock()

	now := time.Now()

	for key, c := range checks {
		limit := now.Add(-c.timeout)
		if c.last.Before(limit) {
			log.Warn("outdated health check", "key", key, "delay", limit.Sub(c.last))
			return false
		}
		log.Debug("last health report", "key", key, "delay", c.last)
	}
	return true
}

func HealthInfo() (bool, string) {
	lock.Lock()
	defer lock.Unlock()

	info := ""
	now := time.Now()
	for key, c := range checks {
		limit := now.Add(-c.timeout)
		info = fmt.Sprintf("%s%s: %s\n", info, key, c.last)
		if c.last.Before(limit) {
			log.Warn("outdated health check", "key", key, "delay", limit.Sub(c.last))
			return false, info
		}
		log.Debug("last health report", "key", key, "delay", c.last)
	}
	return true, info
}
