// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	// "fmt"
	// "bytes"
	"io"
	// "io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "ioping" // For Prometheus metrics.
)

var (
	totalIOs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "io", "total"),
			Help: "Total number of IOs writen to this target.",
		},
		[]string{"target"},
	)

	totalBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "io", "bytes"),
			Help: "Total number of bytes writen to this target.",
		},
		[]string{"target"},
	)

	totalTime = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "io", "millisecs"),
			Help: "Total number of millisec writing to this target.",
		},
		[]string{"target"},
	)

	totalErrs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "io", "errors"),
			Help: "Number of ioping process restart",
		},
		[]string{"target"},
	)

	up = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(namespace, "process", "up"),
			Help: "Wether the ioping process is runnning (=1) or not (=0).",
		},
		[]string{"target"},
	)
)

type metrics map[int]*prometheus.Desc

func (m metrics) String() string {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	s := make([]string, len(keys))
	for i, k := range keys {
		s[i] = strconv.Itoa(k)
	}
	return strings.Join(s, ",")
}

// Exporter collects HAProxy stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	mutex sync.RWMutex
	fetch func() (io.ReadCloser, error)

	totalIOs   *prometheus.CounterVec
	totalBytes *prometheus.CounterVec
	totalTime  *prometheus.CounterVec
	totalErrs  *prometheus.CounterVec
	ioLatency  *prometheus.HistogramVec
	up         *prometheus.GaugeVec
	target     string
	path       string
	logger     log.Logger
	cmd        *exec.Cmd
	stdout     io.ReadCloser
	ch         chan []float64
	done       chan error
}

// NewExporter returns an initialized Exporter.
func NewExporters(options string, targets map[string]string, totalIOs *prometheus.CounterVec, totalBytes *prometheus.CounterVec, totalTime *prometheus.CounterVec, totalErrs *prometheus.CounterVec, ioLatency *prometheus.HistogramVec, up *prometheus.GaugeVec, logger log.Logger) ([]*Exporter, error) {

	exporters := []*Exporter{}
	for name, target := range targets {

		exporter := &Exporter{
			totalIOs:   totalIOs,
			totalBytes: totalBytes,
			totalTime:  totalTime,
			totalErrs:  totalErrs,
			ioLatency:  ioLatency,
			up:         up,
			target:     name,
			path:       target,
			logger:     logger,
			ch:         make(chan []float64, 100),
		}
		exporter.LaunchTargetProcess(target, logger)
		exporters = append(exporters, exporter)
	}
	return exporters, nil

}

func (e *Exporter) LaunchTargetProcess(target string, logger log.Logger) {
	cmd := exec.Command("ioping", "-q", "-p", "1", target)
	e.cmd = cmd

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}
	e.stdout = stdout

	level.Info(logger).Log("msg", "Starting ioping process", "cmd", cmd.String())

	done := make(chan error)
	e.done = done

	err = cmd.Start()
	go func() { done <- cmd.Wait() }()
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}

	go func(e *Exporter) {
		level.Debug(e.logger).Log("msg", "Parsing ioping values")
		se_ch := chan<- []float64(e.ch)

		// Reading all 10 fields, line by line using an internal buffer as a TeeReader
		// reader := e.printAll()
		scanner := bufio.NewScanner(e.stdout)
		scanner.Split(bufio.ScanWords)
		count := 0
		ioping_values := make([]float64, 10)
		for scanner.Scan() {
			count++
			ioping_value, err := strconv.ParseFloat(scanner.Text(), 64)
			if err != nil {
				level.Error(e.logger).Log("msg", "Error while parsing ioping values", "err", err)
				continue
			}
			level.Debug(e.logger).Log("msg", "Got a new value", "value", ioping_value, "count", count)
			ioping_values = append(ioping_values, ioping_value)
			if count == 10 {
				// Flush values into channel and reset
				level.Debug(e.logger).Log("msg", "Flushing a set of values")
				se_ch <- ioping_values
				count = 0
				ioping_values = []float64{}
			}
		}
		level.Debug(e.logger).Log("msg", "Stdout scanner exited", "target", e.target)

	}(e)

}

// Describe describes all the metrics ever exported by the HAProxy exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
}

// Collect fetches the stats from configured HAProxy location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.Lock() // To protect metrics from concurrent collects.
	defer e.mutex.Unlock()

	e.up.WithLabelValues(e.target).Set(e.scrape(ch))
}

// func (e *Exporter) printAll() (buf *bytes.Buffer) {
// 	buf:= bytes.Buffer
// 	tee := io.TeeReader(e.stdout, &buf)
// 	b, err := ioutil.ReadAll(tee)
// 	level.Debug(e.logger).Log("msg", "Read some data from ioping stdout pipe", "n_bytes", len(b))
// 	if err != nil {
// 		level.Error(e.logger).Log("msg", "Error while reading data from ioping stdout pipe", "err", err)
// 	}
// 	return &buf
// }

func (e *Exporter) scrape(ch chan<- prometheus.Metric) (up float64) {
	// Check state of ioping process

	select {
	case err := <-e.done:
		level.Error(e.logger).Log("msg", "ioping process is dead, restarting process ...", "target", e.target)
		if err != nil {
			level.Error(e.logger).Log("msg", "ioping process stopped with the following error", "target", e.target, "err", err)
			e.totalErrs.WithLabelValues(e.target).Inc()
		}
		e.LaunchTargetProcess(e.path, e.logger)
		return 0

	case <-time.After(200 * time.Millisecond):
		// timed out
	}

	level.Debug(e.logger).Log("msg", "Scraping exporter", "target", e.target)

	level.Debug(e.logger).Log("msg", "Scraping exporter", "target", e.target)

	for len(e.ch) > 0 {
		values := <-e.ch
		e.totalIOs.WithLabelValues(e.target).Add(values[0])
		e.totalBytes.WithLabelValues(e.target).Add(values[0] * 4 * 1024)
		e.totalTime.WithLabelValues(e.target).Add(values[5] / 1000000)
		e.ioLatency.WithLabelValues(e.target).Observe(values[5] / 1000000)
	}

	return 1
}

func main() {
	var (
		listenAddress     = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9101").String()
		metricsPath       = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		ioPingOptions     = kingpin.Flag("ioping.options", "Additional options to run ioping with.").Default("").String()
		ioPingStart       = kingpin.Flag("ioping.latency.start", "Start of the exponential distribution of IO lantency in ms").Default("1").Int()
		ioPingLinearCount = kingpin.Flag("ioping.latency.linear_count", "Number of bucket on the linear part of the distribution of IO lantency in ms").Default("20").Int()
		ioPingLinearStep  = kingpin.Flag("ioping.latency.linear_step", "Step of the linear part of the distribution of IO lantency in ms").Default("1").Int()
		ioPingFactor      = kingpin.Flag("ioping.latency.factor", "Factor of the exponential distribution of IO lantency").Default("2").Int()
		ioPingNBuckets    = kingpin.Flag("ioping.latency.buckets", "Number of buckets of the exponential distribution of IO lantency").Default("20").Int()
		ioPingTargets     = kingpin.Arg("ioping.target", "Target filesystem to monitor.").Default("pwd=./").StringMap()
	)

	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)

	level.Info(logger).Log("msg", "Starting ioping_exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", "context", version.BuildContext())

	ioLatencyDistribution := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    prometheus.BuildFQName(namespace, "io", "latency"),
		Help:    "IO Latency distribution for this target",
		Buckets: append(prometheus.LinearBuckets(float64(*ioPingStart), float64(*ioPingLinearStep), *ioPingLinearCount), prometheus.ExponentialBuckets(float64(*ioPingStart)+float64(*ioPingLinearStep**ioPingLinearCount), float64(*ioPingFactor), *ioPingNBuckets)...)},
		[]string{"target"},
	)

	exporters, err := NewExporters(*ioPingOptions, *ioPingTargets, totalIOs, totalBytes, totalTime, totalErrs, ioLatencyDistribution, up, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating an exporter", "err", err)
		os.Exit(1)
	}
	prometheus.MustRegister(totalIOs)
	prometheus.MustRegister(totalBytes)
	prometheus.MustRegister(totalTime)
	prometheus.MustRegister(totalErrs)
	prometheus.MustRegister(ioLatencyDistribution)
	prometheus.MustRegister(up)
	for _, e := range exporters {
		prometheus.MustRegister(e)
		level.Info(logger).Log("msg", "Register a new ioping exporter", "err", err)
	}
	prometheus.MustRegister(version.NewCollector("ioping_exporter"))

	level.Info(logger).Log("msg", "Listening on address", "address", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>ioPing Exporter</title></head>
             <body>
             <h1>ioPing Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	if err := http.ListenAndServe(*listenAddress, nil); err != nil {
		level.Error(logger).Log("msg", "Error starting HTTP server", "err", err)
		os.Exit(1)
	}
}
