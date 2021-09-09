package nearcache

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
