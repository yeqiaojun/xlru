package xlru

import (
	"sync/atomic"
	"time"
)

const clockInterval = 10 * time.Millisecond

var clockEpoch = time.Now()
var clockUpper atomic.Int64

// One shared clock keeps system-time reads out of ordinary TTL hits. Near a
// deadline, readers use exact monotonic time. As with any cached clock, scheduler
// stalls may delay observing expiration until the updater runs again.
func init() {
	clockUpper.Store(int64(clockInterval))
	go func() {
		for range time.Tick(clockInterval) {
			clockUpper.Store(time.Since(clockEpoch).Nanoseconds() + int64(clockInterval))
		}
	}()
}
