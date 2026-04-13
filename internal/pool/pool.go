// Package pool provides object pooling for hot-path allocations.
// Reusing LogBatch and LogRecord slices keeps GC pressure low,
// which is critical for a sidecar that must stay within tight RAM budgets.
package pool

import (
	"sync"

	pb "github.com/TellurianCorp/watchtower/proto"
)

var batchPool = sync.Pool{
	New: func() any {
		return &pb.LogBatch{
			Records: make([]*pb.LogRecord, 0, 128),
		}
	},
}

// GetBatch retrieves a reusable LogBatch from the pool.
func GetBatch() *pb.LogBatch {
	return batchPool.Get().(*pb.LogBatch)
}

// PutBatch returns a LogBatch to the pool after clearing its contents.
func PutBatch(b *pb.LogBatch) {
	b.Records = b.Records[:0]
	b.Metadata = nil
	batchPool.Put(b)
}
