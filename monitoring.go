package cloudplatform

import (
	"encoding/json"
	"errors"
	"github.com/mozilla-services/heka/pipeline"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/cloudmonitoring/v2beta2"
	"google.golang.org/cloud/compute/metadata"
	"log"
	"net/http"
	"time"
)

type CloudMonitoringConfig struct {
	ProjectId     string `toml:"project_id"`
	Zone          string `toml:"zone"`
	ResourceId    string `toml:"resource_id"`
	FlushInterval uint32 `toml:"flush_interval"`
	FlushCount    int    `toml:"flush_count"`
}

type CloudMonitoringOutput struct {
	conf       *CloudMonitoringConfig
	client     *http.Client
	service    *cloudmonitoring.Service
	backChan   chan []*cloudmonitoring.TimeseriesPoint
	batchChan  chan MonitoringBatch // Chan to pass completed batches
	outputExit chan error
	or         pipeline.OutputRunner
}

type MonitoringBatch struct {
	count int64
	batch []*cloudmonitoring.TimeseriesPoint
}

func (cmo *CloudMonitoringOutput) ConfigStruct() interface{} {
	return &CloudMonitoringConfig{Zone: "us-central1-a", ResourceId: "0"}
}

func (cmo *CloudMonitoringOutput) Init(config interface{}) (err error) {
	cmo.conf = config.(*CloudMonitoringConfig)

	if metadata.OnGCE() {
		if cmo.conf.ProjectId == "" {
			if cmo.conf.ProjectId, err = metadata.ProjectID(); err != nil {
				return
			}
		}
		if cmo.conf.ResourceId == "" {
			if cmo.conf.ResourceId, err = metadata.InstanceID(); err != nil {
				return
			}
		}
		if cmo.conf.Zone == "" {
			if cmo.conf.Zone, err = metadata.Get("instance/zone"); err != nil {
				return
			}
		}
	}
	if cmo.conf.ProjectId == "" {
		return errors.New("ProjectId cannot be blank")
	}

	cmo.batchChan = make(chan MonitoringBatch)
	cmo.backChan = make(chan []*cloudmonitoring.TimeseriesPoint, 2)
	cmo.outputExit = make(chan error)
	if cmo.client, err = google.DefaultClient(oauth2.NoContext,
		cloudmonitoring.MonitoringScope); err != nil {
		return
	}
	if cmo.service, err = cloudmonitoring.New(cmo.client); err != nil {
		return
	}
	r := &cloudmonitoring.ListMetricDescriptorsRequest{Kind: "cloudmonitoring#listMetricDescriptorsRequest"}
	_, err = cmo.service.MetricDescriptors.List(cmo.conf.ProjectId, r).Do()
	if err != nil {
		log.Print("Init CloudMonitoringOutput Error: %v", err)
	}
	return
}

func (cmo *CloudMonitoringOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	var (
		pack     *pipeline.PipelinePack
		e        error
		m        *cloudmonitoring.TimeseriesPoint
		ok       = true
		count    int64
		inChan   = or.InChan()
		outBatch = make([]*cloudmonitoring.TimeseriesPoint, 0, 200)
		ticker   = time.Tick(time.Duration(cmo.conf.FlushInterval) * time.Millisecond)
	)
	cmo.or = or
	go cmo.committer()
	for ok {
		select {
		case pack, ok = <-inChan:
			// Closed inChan => we're shutting down, flush data.
			if !ok {
				if len(outBatch) > 0 {
					cmo.sendBatch(outBatch, count)
				}
				close(cmo.batchChan)
				<-cmo.outputExit
				break
			}

			m, e = cmo.Encode(pack)
			pack.Recycle()
			if e != nil {
				or.LogError(e)
				continue
			}

			if m != nil {
				outBatch = append(outBatch, m)

				if count++; cmo.CheckFlush(int(count), len(outBatch)) {
					if len(outBatch) > 0 {
						outBatch = cmo.sendBatch(outBatch, count)
						count = 0
					}
				}
			}
		case <-ticker:
			if len(outBatch) > 0 {
				outBatch = cmo.sendBatch(outBatch, count)
			}
			count = 0
		case err = <-cmo.outputExit:
			ok = false
		}
	}
	return
}

func (cmo *CloudMonitoringOutput) committer() {
	cmo.backChan <- make([]*cloudmonitoring.TimeseriesPoint, 0, 100)

	for b := range cmo.batchChan {
		if err := cmo.SendRecord(b.batch); err != nil {
			cmo.or.LogError(err)
		}
		b.batch = b.batch[:0]
		cmo.backChan <- b.batch
	}
	cmo.outputExit <- nil
}

func (cmo *CloudMonitoringOutput) SendRecord(entries []*cloudmonitoring.TimeseriesPoint) (err error) {
	r := &cloudmonitoring.WriteTimeseriesRequest{Timeseries: entries}
	_, err = cmo.service.Timeseries.Write(cmo.conf.ProjectId, r).Do()
	if err != nil {
		log.Print("Write Timeseries Error: %v", err)
	}
	return
}

func (cmo *CloudMonitoringOutput) sendBatch(entries []*cloudmonitoring.TimeseriesPoint, count int64) (nextBatch []*cloudmonitoring.TimeseriesPoint) {
	// This will block until the other side is ready to accept
	// this batch, so we can't get too far ahead.
	b := MonitoringBatch{
		count: count,
		batch: entries,
	}
	cmo.batchChan <- b
	nextBatch = <-cmo.backChan
	return nextBatch
}

func (cmo *CloudMonitoringOutput) CheckFlush(count int, length int) bool {
	if count >= cmo.conf.FlushCount {
		return true
	}
	return false
}

func (cmo *CloudMonitoringOutput) Encode(pack *pipeline.PipelinePack) (entry *cloudmonitoring.TimeseriesPoint, err error) {
	var (
		iName interface{}
		name  string
		point = &cloudmonitoring.Point{}
		ok    bool
	)
	message := pack.Message
	iName, ok = message.GetFieldValue("MetricName")
	if !ok {
		err = errors.New("No MetricName field")
		return
	}
	if name, ok = iName.(string); name == "" || !ok {
		err = errors.New("MetricName is invalid")
		return
	}

	if err = json.Unmarshal([]byte(message.GetPayload()), point); err != nil {
		return
	}

	meta := &cloudmonitoring.TimeseriesDescriptor{
		Project: cmo.conf.ProjectId,
		Metric:  name,
	}

	entry = &cloudmonitoring.TimeseriesPoint{TimeseriesDesc: meta, Point: point}
	return
}

func init() {
	pipeline.RegisterPlugin("CloudMonitoringOutput", func() interface{} {
		return new(CloudMonitoringOutput)
	})
}
