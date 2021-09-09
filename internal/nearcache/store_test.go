package nearcache

import (
	"sync"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"

	"github.com/stretchr/testify/assert"
)

func TestSmoke(t *testing.T) {
	rs := RecordStore{
		store:         sync.Map{},
		reservationID: new(int64),
	}

	actual, err := rs.get(1)
	assert.NoError(t, err)
	assert.Equal(t, nil, actual)

	reservation := rs.tryReserveForUpdate(1)
	assert.NotEqual(t, reservation, NotReservedID)

	actual, err = rs.get(1)
	assert.NoError(t, err)
	assert.Equal(t, nil, actual)

	record := rs.tryPublishReserved(1, []byte("updated"), reservation)
	assert.NotNil(t, record)

	actual, err = rs.get(1)
	dr, ok := actual.(*serialization.Data)
	assert.True(t, ok)
	assert.Equal(t, []byte("updated"), dr.Payload)
}
