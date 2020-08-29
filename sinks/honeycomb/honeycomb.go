package honeycomb

import (
	"sync"
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	
	"github.com/stripe/veneur/protocol"
	"github.com/stripe/veneur/sinks"
	"github.com/stripe/veneur/ssf"
	"github.com/stripe/veneur/trace"
	"github.com/stripe/veneur/trace/metrics"
)

type HoneycombSpanSink struct {
	APIKey string
	APIHost string
	Dataset string
	client *libhoney.Client
	mutex *sync.Mutex
	traceClient *trace.Client
	spansSinceLastFlush map[string]int64
}

func NewHoneycombSpanSink() (*HoneycombSpanSink, error) {
	return &HoneycombSpanSink{
		// ...
	}, nil
}

func (hc *HoneycombSpanSink) Name() string {
	return "honeycomb"
}

func (hc *HoneycombSpanSink) Start(tc *trace.Client) error {
	hc.traceClient = tc
	client, err := libhoney.NewClient(libhoney.ClientConfig{
		APIKey: hc.APIKey,
		APIHost: hc.APIHost,
		Dataset: hc.Dataset,
	})
	if err != nil {
		return err
	}
	hc.client = client
	hc.spansSinceLastFlush = make(map[string]int64)
	return nil
}

// Construct a libhoney event and forward it to the backend. It will be sent
// via the Honeycomb API asynchronously.
func (hc *HoneycombSpanSink) Ingest(span *ssf.SSFSpan) error {
	if err := protocol.ValidateTrace(span); err != nil {
		return err
	}

	spanEvent := hc.buildSpanEvent(span)
	spanEvent.Send()

	for _, metric := range span.Metrics {
		metricEvent := hc.buildMetricEvent(metric, span.Id)
		metricEvent.Send()
	}

	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	hc.spansSinceLastFlush[span.Service] += 1

	return nil
}

func (hc *HoneycombSpanSink) buildSpanEvent(span *ssf.SSFSpan) *libhoney.Event {
	event := hc.client.NewEvent()
	baseProperties := map[string]interface{}{
		"trace_id": span.TraceId,
		"span_id": span.Id,
		"start_timestamp": span.StartTimestamp,
		"end_timestamp": span.EndTimestamp,
		"name": span.Name,
		"error": span.Error,
		"service": span.Service,
	}

	if span.ParentId != 0 {
		baseProperties["parent_id"] = span.ParentId
	}

	if span.RootStartTimestamp != 0 {
		baseProperties["root_start_timestamp"] = span.RootStartTimestamp
	}

	event.Add(baseProperties)
	event.Add(span.Tags)
	event.Timestamp = hc.convertTimestamp(span.StartTimestamp)

	return event
}

func (hc *HoneycombSpanSink) buildMetricEvent(metric *ssf.SSFSample, parentSpanId int64) *libhoney.Event {
	event := hc.client.NewEvent()

	event.Add(map[string]interface{}{
		"name": metric.Name,
		"value": metric.Value,
		"message": metric.Message,
		"status": metric.Status.String(),
		"unit": metric.Unit,
		"type": metric.Metric.String(),
	})
	event.Add(metric.Tags)
	event.SampleRate = hc.convertSampleRate(metric.SampleRate)
	event.Timestamp = hc.convertTimestamp(metric.Timestamp)

	return event
}

func (hc *HoneycombSpanSink) convertSampleRate(ssfSampleRate float32) uint {
	if 0.0 < ssfSampleRate && ssfSampleRate <= 1.0 {
		return uint(1/ssfSampleRate)
	} else {
		return 1
	}
}

func (hc* HoneycombSpanSink) convertTimestamp(nanosSinceEpoch int64) time.Time {
	return time.Unix(nanosSinceEpoch / 1e9, nanosSinceEpoch % 1e9)
}

// libhoney sends events asynchronously, so just emit some metrics on each flush
func (hc *HoneycombSpanSink) Flush() {
	samples := &ssf.Samples{}
	defer metrics.Report(hc.traceClient, samples)

	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	for service, count := range hc.spansSinceLastFlush {
		samples.Add(
			ssf.Count(sinks.MetricKeyTotalSpansFlushed, float32(count), map[string]string{
				"sink": hc.Name(),
				"service": service,
			}),
		)
	}

	hc.spansSinceLastFlush = make(map[string]int64)
}