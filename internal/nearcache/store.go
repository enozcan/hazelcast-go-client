package nearcache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

var TypeFormatErr = errors.New("format mismatch")


type RecordStore struct {
	store         sync.Map // interface{} -> *DataRecord
	reservationID *int64
}

func (s *RecordStore) get(key interface{}) (interface{}, error) {
	cached, ok := s.store.Load(key)
	if !ok {
		s.incrementMisses()
		return nil, nil
	}
	record, ok := cached.(*DataRecord)
	if !ok {
		return nil, TypeFormatErr
	}
	if record.getReservationID() != ReadPermittedID {
		s.incrementMisses()
		return nil, nil
	}

	// TODO: Check stale read detector

	if record.expired() {
		// TODO: invalidate key
		s.incrementExpirations()
		return nil, nil
	}
	record.onAccess()
	s.incrementHits()
	return record.data, nil
}

func (s *RecordStore) invalidate(key interface{}) {
	s.store.Delete(key)
}

func (s *RecordStore) tryReserveForUpdate(key interface{}) int64 {
	resID := s.nextReservationID()
	val := DataRecord{
		creationTime:   time.Now(),
		expiryTime:     time.Now().Add(time.Hour),
		cachedAsNil:    false,
		mu:             sync.Mutex{},
		lastAccessTime: time.Now(),
		hits:           0,
		reservationID:  resID,
		data:           nil,
	}
	if _, loaded := s.store.LoadOrStore(key, &val); loaded {
		// Value has already been reserved or written.
		return NotReservedID
	}
	// Reservation complete. resID can now update the value.
	return resID
}

func (s *RecordStore) tryPublishReserved(key interface{}, data []byte, reservationID int64) *DataRecord {
	cached, ok := s.store.Load(key)
	if !ok {
		// deleted during update reservation
		return nil
	}
	record, _ := cached.(*DataRecord)
	resID := record.getReservationID()
	if resID != reservationID {
		// record has been deleted then updated. Return the latest value.
		if resID != ReadPermittedID {
			// still updating the value. Return nil from near cache.
			return nil
		}
		return record
	}
	record.data = serialization.NewData(data)
	record.setReservationID(ReadPermittedID) // anyone can read the data from now on.
	// TODO: read the latest value here or read-your-write?
	return record
}

func (s *RecordStore) nextReservationID() int64 {
	return atomic.AddInt64(s.reservationID, 1)
}

func (s *RecordStore) incrementMisses() {
	fmt.Println("incrementMisses")
}

func (s *RecordStore) incrementExpirations() {
	fmt.Println("incrementExpirations")
}

func (s *RecordStore) incrementHits() {
	fmt.Println("incrementHits")
}
