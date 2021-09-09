package nearcache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const NotReservedID int64 = -2
const ReadPermittedID int64 = -1

type DataRecord struct {
	creationTime time.Time
	expiryTime   time.Time
	cachedAsNil  bool

	mu             sync.Mutex
	lastAccessTime time.Time
	hits           int64

	reservationID int64 // access must be atomic
	data          *serialization.Data // no update happens once reservationID is set to ReadPermittedID
	// TODO: support deserialized cached values
}

func (d *DataRecord) getReservationID() int64 {
	return atomic.LoadInt64(&d.reservationID)
}

func (d *DataRecord) setReservationID(value int64) {
	atomic.StoreInt64(&d.reservationID, value)
}

func (d *DataRecord) isCachedAsNil() bool {
	return d.cachedAsNil
}

func (d *DataRecord) expired() bool {
	return time.Now().After(d.expiryTime)
}

func (d *DataRecord) onAccess() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastAccessTime = time.Now()
	d.hits++
}
