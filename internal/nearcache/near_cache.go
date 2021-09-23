package nearcache

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type NearCache struct {
	nearCacheRecordStore RecordStore
	serializeKeys        bool
}

func (n *NearCache) get(key interface{}) (interface{}, error) {
	return n.nearCacheRecordStore.get(key)
}

func (n *NearCache) tryReserveForUpdate(key interface{}) int64 {
	return n.nearCacheRecordStore.tryReserveForUpdate(key)
}

func (n *NearCache) tryPublishReserved(key interface{}, data []byte, reservationID int64) interface{} {
	return n.nearCacheRecordStore.tryPublishReserved(key, data, reservationID)
}

func (n *NearCache) newInvalidationListener(name string, localOnly bool) (subsID types.UUID, add *proto.ClientMessage, remove *proto.ClientMessage, handler func(clientMessage *proto.ClientMessage)) {
	// TODO: to be registered in proxy.listenerBinder
	subsID = types.NewUUID()
	add = codec.EncodeMapAddNearCacheInvalidationListenerRequest(name, int32(hazelcast.EntryInvalidated), localOnly)
	remove = codec.EncodeMapRemoveEntryListenerRequest(name, subsID)
	handler = func(clientMessage *proto.ClientMessage) {
		codec.HandleMapAddNearCacheInvalidationListener(clientMessage, n.handleIMapInvalidationEvent, n.handleIMapBatchInvalidationEvent)
	}
	return
}

func (n *NearCache) handleIMapInvalidationEvent(key *serialization.Data, sourceUuid types.UUID, partitionUuid types.UUID, sequence int64) {
	// TODO: invalidate single key in the store
}

func (n *NearCache) handleIMapBatchInvalidationEvent(keys []*serialization.Data, sourceUuids []types.UUID, partitionUuids []types.UUID, sequences []int64) {
	// TODO: invalidate all keys in the store
}
