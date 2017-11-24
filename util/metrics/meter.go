package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"util/gogc"
)

type ApiMetric struct {
	name                   string
	numberOfRequest        int64
	numberOfErrResponse    int64
	summaryDelayOfResponse int64
	maxDelayOfResponse     time.Duration
	minDelayOfResponse     time.Duration
}

const (
	RUNNING = int32(1)
	STOPPED = int32(0)
)

type MetricMeter struct {
	// host or other name that is not repeated
	name  string
	run   int32
	mutex *sync.RWMutex

	metrics   map[string]*ApiMetric
	timestamp time.Time
	avgTotal  float64
	lats      []float64
	output    Output
}

func NewMetricMeter(name string, output Output) *MetricMeter {
	if output == nil {
		return nil
	}
	meter := &MetricMeter{
		name:      name,
		run:       STOPPED,
		mutex:     new(sync.RWMutex),
		metrics:   make(map[string]*ApiMetric),
		timestamp: time.Now(),
		output:    output,
		lats:      make([]float64, 0, 100000),
	}
	go meter.Run()
	return meter
}

func (this *MetricMeter) Run() {
	var interval, waitTime time.Duration
	if atomic.LoadInt32(&this.run) == STOPPED {
		atomic.StoreInt32(&this.run, RUNNING)

		this.timestamp = time.Now()
		interval = this.output.ReportInterval()
		waitTime = interval
		for atomic.LoadInt32(&this.run) == RUNNING {
			time.Sleep(waitTime)
			tag := time.Now()
			this.reportAndReset()
			waitTime = interval - (time.Now().Sub(tag))
			if waitTime < 0 {
				waitTime = 0
			}
		}
	}
}

func (this *MetricMeter) Stop() {
	if this == nil {
		return
	}
	atomic.StoreInt32(&this.run, STOPPED)
}

func (this *MetricMeter) findMetric(method string) (*ApiMetric, bool) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	if metric, ok := this.metrics[method]; ok {
		return metric, true
	}
	return nil, false
}

func (this *MetricMeter) AddApi(reqMethod string, ack bool) {
	if this == nil {
		return
	}
	method := reqMethod
	metric, ok := this.findMetric(method)
	if !ok {
		this.mutex.Lock()
		metric, ok = this.metrics[method]
		if !ok {
			metric = new(ApiMetric)
			metric.name = method
			this.metrics[method] = metric
		}
		this.mutex.Unlock()
	}

	atomic.AddInt64(&metric.numberOfRequest, 1)
	if !ack {
		atomic.AddInt64(&metric.numberOfErrResponse, 1)
	}
}

func (this *MetricMeter) AddApiWithDelay(reqMethod string, ack bool, delay time.Duration) {
	if this == nil {
		return
	}
	method := reqMethod
	metric, ok := this.findMetric(method)
	if !ok {
		this.mutex.Lock()
		metric, ok = this.metrics[method]
		if !ok {
			metric = new(ApiMetric)
			metric.name = method
			metric.maxDelayOfResponse = delay
			metric.minDelayOfResponse = delay
			this.metrics[method] = metric
		}
		this.mutex.Unlock()
	}
	atomic.AddInt64(&metric.numberOfRequest, 1)
	if !ack {
		atomic.AddInt64(&metric.numberOfErrResponse, 1)
	}
	atomic.AddInt64(&metric.summaryDelayOfResponse, int64(delay))
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if metric.maxDelayOfResponse < delay {
		metric.maxDelayOfResponse = delay
	}
	if metric.minDelayOfResponse > delay {
		metric.minDelayOfResponse = delay
	}
	this.lats = append(this.lats, delay.Seconds())
	this.avgTotal += delay.Seconds()
}

type OpenFalconCustomData struct {
	Name        string  `json:"name"`
	Metric      string  `json:"metric"`
	Timestamp   int64   `json:"timestamp"`
	Step        int64   `json:"step"`
	Value       float64 `json:"value"`
	CounterType string  `json:"counterType"`
	Tags        string  `json:"tags"`
}

func NewCustomData(name, metric string, timestamp, step int64, value float64, counterType, tags string) *OpenFalconCustomData {
	var data OpenFalconCustomData
	data.Name = name
	data.Metric = metric
	data.Timestamp = timestamp
	data.Step = step
	data.Value = value
	data.CounterType = counterType
	data.Tags = tags
	return &data
}

func (this *MetricMeter) report2OpenFalcon(total time.Duration, lats []float64, avgTotal float64, metrics map[string]*ApiMetric) {
	sort.Float64s(lats)
	timestamp := time.Now().Unix()

	for method, metric := range metrics {
		customDataList := make([]*OpenFalconCustomData, 0, 6)

		numberOfResponse := metric.numberOfRequest
		averageDelayOfResponse := time.Duration(0)
		if numberOfResponse > 0 {
			averageDelayOfResponse = time.Duration(int64(metric.summaryDelayOfResponse) / numberOfResponse)
		}

		customDataList = append(customDataList, NewCustomData(this.name, "number_of_request", timestamp, 60, float64(metric.numberOfRequest), "GAUGE", "item="+this.name+",api="+method))
		customDataList = append(customDataList, NewCustomData(this.name, "number_of_err_response", timestamp, 60, float64(metric.numberOfErrResponse), "GAUGE", "item="+this.name+",api="+method))
		customDataList = append(customDataList, NewCustomData(this.name, "average_delay_of_response", timestamp, 60, float64(averageDelayOfResponse.Seconds()), "GAUGE", "item="+this.name+",api="+method))
		customDataList = append(customDataList, NewCustomData(this.name, "max_delay_of_response", timestamp, 60, float64(metric.maxDelayOfResponse.Seconds()), "GAUGE", "item="+this.name+",api="+method))
		customDataList = append(customDataList, NewCustomData(this.name, "min_delay_of_response", timestamp, 60, float64(metric.minDelayOfResponse.Seconds()), "GAUGE", "item="+this.name+",api="+method))

		customDataJson, err := json.Marshal(customDataList)
		if err != nil {
			continue
		}

		this.output.Report(customDataJson)
	}
	if len(lats) > 0 {
		rps := float64(len(lats)) / total.Seconds()
		average := avgTotal / float64(len(lats))

		customDataList := make([]*OpenFalconCustomData, 0, 11)
		customDataList = append(customDataList, NewCustomData(this.name, "rps", timestamp, 60, rps, "GAUGE", "item="+this.name))
		customDataList = append(customDataList, NewCustomData(this.name, "average_latency", timestamp, 60, average, "GAUGE", "item="+this.name))
		pctls := []int{100, 250, 500, 750, 900, 950, 990, 999}
		data := make([]float64, len(pctls))
		j := 0
		for i := 0; i < len(lats) && j < len(pctls); i++ {
			current := (i * 1000) / len(lats)
			if current >= pctls[j] {
				data[j] = lats[i]
				j++
			}
		}
		for i := 0; i < len(pctls); i++ {
			if data[i] > 0 {
				var metric string
				l := pctls[i] / 10
				m := pctls[i] % 10
				if m == 0 {
					metric = fmt.Sprintf("tp%d", l)
				} else {
					metric = fmt.Sprintf("tp%d%d", l, m)
				}
				customDataList = append(customDataList, NewCustomData(this.name, metric, timestamp, 60, data[i], "GAUGE", "item="+this.name))
			}
		}
		customDataJson, err := json.Marshal(customDataList)
		if err != nil {
			fmt.Println("json marshal error ", err)
			return
		}

		this.output.Report(customDataJson)
	}
}

func (this *MetricMeter) reportGc() {
	writer := NewBufferWriter()
	gogc.PrintGCSummary(writer)
	this.output.Report(writer.Get())
}

func (this *MetricMeter) reportAndReset() {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				fmt.Printf("Error: %s.\n", x)
			case error:
				fmt.Printf("Error: %s.\n", x.Error())
			default:
				fmt.Printf("Unknown panic error.")
			}
		}
	}()

	this.mutex.Lock()
	metrics := this.metrics
	total := time.Now().Sub(this.timestamp)
	avgTotal := this.avgTotal
	lats := this.lats
	this.metrics = make(map[string]*ApiMetric)
	this.timestamp = time.Now()
	this.lats = make([]float64, 0, 100000)
	this.avgTotal = 0
	this.mutex.Unlock()

	if len(metrics) == 0 {
		this.reportGc()
		return
	}
	this.report2OpenFalcon(total, lats, avgTotal, metrics)
	this.reportGc()
}

type BufferWriter struct {
	lock   sync.Mutex
	offset int
	buff   []byte
}

func NewBufferWriter() *BufferWriter {
	return &BufferWriter{buff: make([]byte, 10240)}
}

func (b *BufferWriter) Write(p []byte) (n int, err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if len(b.buff[b.offset:]) < len(p) {
		return 0, errors.New("buffer size limit")
	}
	copy(b.buff[b.offset:], p)
	b.offset += len(p)
	return len(p), nil
}

func (b *BufferWriter) Get() []byte {
	b.lock.Lock()
	defer b.lock.Unlock()
	buff := make([]byte, len(b.buff[:b.offset]))
	copy(buff, b.buff[:b.offset])
	return buff
}

func (b *BufferWriter) Read(p []byte) (n int, err error) {
	//b.lock.Lock()
	//defer b.lock.Unlock()
	//if len(b.buff)
	return 0, nil
}
