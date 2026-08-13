package segments

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/conc"
)

type doOnSegmentFunc func(ctx context.Context, segment Segment) error
type latencyTraceOperationKey struct{}

func withLatencyTraceOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, latencyTraceOperationKey{}, operation)
}

func getLatencyTraceOperation(ctx context.Context) string {
	operation, _ := ctx.Value(latencyTraceOperationKey{}).(string)
	return operation
}

func recordSegmentCacheAccess(ctx context.Context, operation string, segment Segment, hit bool) {
	stage := "segment_cache_hit"
	source := "querynode_cache"
	if !hit {
		stage = "segment_cache_miss"
		source = "object_store"
	}
	tracer.GetGlobalTracer().RecordEvent(
		tracer.GetTraceIDFromContext(ctx),
		"",
		operation,
		stage,
		"querynode",
		0,
		map[string]interface{}{
			"source":        source,
			"collection_id": segment.Collection(),
			"partition_id":  segment.Partition(),
			"segment_id":    segment.ID(),
			"segment_type":  segment.Type().String(),
		},
	)
}

func doOnSegment(ctx context.Context, mgr *Manager, seg Segment, do doOnSegmentFunc) error {
	// record search time and cache miss
	var err error
	accessRecord := metricsutil.NewQuerySegmentAccessRecord(getSegmentMetricLabel(seg))
	defer func() {
		accessRecord.Finish(err)
	}()
	if seg.IsLazyLoad() {
		ctx, cancel := withLazyLoadTimeoutContext(ctx)
		defer cancel()

		var missing bool
		traceCtx := withLatencyTraceOperation(ctx, "Query")
		missing, err = mgr.DiskCache.Do(traceCtx, seg.ID(), do)
		if missing {
			accessRecord.CacheMissing()
		}
		recordSegmentCacheAccess(traceCtx, "Query", seg, !missing)
		if err != nil {
			log.Ctx(ctx).Warn("failed to do query disk cache", zap.Int64("segID", seg.ID()), zap.Error(err))
		}
		return err
	}
	return do(ctx, seg)
}

// doOnSegments Be careful to use this, since no any pool is used.
func doOnSegments(ctx context.Context, mgr *Manager, segments []Segment, do doOnSegmentFunc) error {
	errGroup, ctx := errgroup.WithContext(ctx)
	for _, segment := range segments {
		seg := segment
		errGroup.Go(func() error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return doOnSegment(ctx, mgr, seg, do)
		})
	}
	return errGroup.Wait()
}

func doOnSegmentsWithPool(ctx context.Context, mgr *Manager, segments []Segment, do doOnSegmentFunc, pool *conc.Pool[any]) error {
	futures := make([]*conc.Future[any], 0, len(segments))
	for _, segment := range segments {
		seg := segment
		future := pool.Submit(func() (any, error) {
			err := doOnSegment(ctx, mgr, seg, do)
			return nil, err
		})
		futures = append(futures, future)
	}
	return conc.BlockOnAll(futures...)
}
