package main

import (
	"database/sql"
	"flag"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-oci8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"strconv"
)

var (
	// Version will be set at build time.
	Version       = "0.0.0.dev"
	listenAddress = flag.String("web.listen-address", ":9161", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	landingPage   = []byte("<html><head><title>Oracle DB exporter</title></head><body><h1>Oracle DB exporter</h1><p><a href='" + *metricPath + "'>Metrics</a></p></body></html>")
)

// Metric name parts.
const (
	namespace = "oracledb"
	exporter  = "exporter"
)

// Exporter collects Oracle DB metrics. It implements prometheus.Collector.
type Exporter struct {
	dsn             string
	duration, error prometheus.Gauge
	totalScrapes    prometheus.Counter
	scrapeErrors    *prometheus.CounterVec
	up              prometheus.Gauge
}

// NewExporter returns a new Oracle DB exporter for the provided DSN.
func NewExporter(dsn string) *Exporter {
	return &Exporter{
		dsn: dsn,
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from Oracle DB.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times Oracle DB was scraped for metrics.",
		}),
		scrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occured scraping a Oracle database.",
		}, []string{"collector"}),
		error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from Oracle DB resulted in an error (1 for error, 0 for success).",
		}),
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the Oracle database server is up.",
		}),
	}
}

// Describe describes all the metrics exported by the MS SQL exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the Oracle DB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored Oracle instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh

}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(ch)
	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.error
	e.scrapeErrors.Collect(ch)
	ch <- e.up
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	e.totalScrapes.Inc()
	var err error
	defer func(begun time.Time) {
		e.duration.Set(time.Since(begun).Seconds())
		if err == nil {
			e.error.Set(0)
		} else {
			e.error.Set(1)
		}
	}(time.Now())

	db, err := sql.Open("oci8", e.dsn)
	if err != nil {
		log.Errorln("Error opening connection to database:", err)
		return
	}
	defer db.Close()

	isUpRows, err := db.Query("SELECT 1 FROM DUAL")
	if err != nil {
		log.Errorln("Error pinging oracle:", err)
		e.up.Set(0)
		return
	}
	isUpRows.Close()
	e.up.Set(1)

	if err = ScrapeActivity(db, ch); err != nil {
		log.Errorln("Error scraping for activity:", err)
		e.scrapeErrors.WithLabelValues("activity").Inc()
	}

	if err = ScrapeWaitTime(db, ch); err != nil {
		log.Errorln("Error scraping for wait_time:", err)
		e.scrapeErrors.WithLabelValues("wait_time").Inc()
	}

	if err = ScrapeSessions(db, ch); err != nil {
		log.Errorln("Error scraping for sessions:", err)
		e.scrapeErrors.WithLabelValues("sessions").Inc()
	}

	if err = ScrapeAQ(db, ch); err != nil {
		log.Errorln("Error scraping for Oracle AQ:", err)
		e.scrapeErrors.WithLabelValues("oracleaq").Inc()
	}

	if err = ScrapeASM(db, ch); err != nil {
		log.Errorln("Error scraping for Oracle ASM iostat:", err)
		e.scrapeErrors.WithLabelValues("oracleasm").Inc()
	}
}

// ScrapeSessions collects session metrics from the v$session view.
func ScrapeSessions(db *sql.DB, ch chan<- prometheus.Metric) error {
	var err error
	var activeCount float64
	var inactiveCount float64

	// There is probably a better way to do this with a single query. #FIXME when I figure that out.
	err = db.QueryRow("SELECT COUNT(*) FROM v$session WHERE status = 'ACTIVE'").Scan(&activeCount)
	if err != nil {
		return err
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "active"),
			"Gauge metric with count of sessions marked ACTIVE", []string{}, nil),
		prometheus.GaugeValue,
		activeCount,
	)

	err = db.QueryRow("SELECT COUNT(*) FROM v$session WHERE status = 'INACTIVE'").Scan(&inactiveCount)
	if err != nil {
		return err
	}

	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(prometheus.BuildFQName(namespace, "sessions", "inactive"),
			"Gauge metric with count of sessions marked INACTIVE.", []string{}, nil),
		prometheus.GaugeValue,
		inactiveCount,
	)

	return nil
}

// ScrapeWaitTime collects wait time metrics from the v$waitclassmetric view.
func ScrapeWaitTime(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT n.wait_class, round(m.time_waited/m.INTSIZE_CSEC,3) AAS from v$waitclassmetric  m, v$system_wait_class n where m.wait_class_id=n.wait_class_id and n.wait_class != 'Idle'")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		name = cleanName(name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "wait_time", name),
				"Generic counter metric from v$waitclassmetric view in Oracle.", []string{}, nil),
			prometheus.CounterValue,
			value,
		)
	}
	return nil
}

// ScrapeActivity collects activity metrics from the v$sysstat view.
func ScrapeActivity(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT name, value FROM v$sysstat WHERE name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var value float64
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		name = cleanName(name)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "activity", name),
				"Generic counter metric from v$sysstat view in Oracle.", []string{}, nil),
			prometheus.CounterValue,
			value,
		)
	}
	return nil
}

// ScrapeAQ collects oracle aq metrics fr om v$persistent_queues view.
func ScrapeAQ(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT queue_name, enqueued_msgs, dequeued_msgs, elapsed_enqueue_time, elapsed_dequeue_time, enqueue_cpu_time, dequeue_cpu_time FROM v$persistent_queues")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var queueName string
		var enqueuedMsgs float64
		var dequeuedMsgs float64
		var elapsedEnqueueTime float64
		var elapsedDequeueTime float64
		var enqueueCpuTime float64
		var dequeueCpuTime float64
		if err = rows.Scan(&queueName, &enqueuedMsgs, &dequeuedMsgs, &elapsedEnqueueTime, &elapsedDequeueTime, &enqueueCpuTime, &dequeueCpuTime); err != nil {
			return err
		}
		queueName = cleanName(queueName)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "enqueue", "queue": queueName}),
			prometheus.CounterValue,
			enqueuedMsgs,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "dequeue", "queue": queueName}),
			prometheus.CounterValue,
			dequeuedMsgs,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "elapsedenqueuetime", "queue": queueName}),
			prometheus.CounterValue,
			elapsedEnqueueTime,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "elapseddequeuetime", "queue": queueName}),
			prometheus.CounterValue,
			elapsedDequeueTime,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "enqueuecputime", "queue": queueName}),
			prometheus.CounterValue,
			enqueueCpuTime,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "aq", "count"), "Generic counter metric from v$persistent_queues view in Oracle", []string{}, prometheus.Labels{"method": "dequeuecputime", "queue": queueName}),
			prometheus.CounterValue,
			dequeueCpuTime,
		)

	}
	return nil
}

// ScrapeASM collects oracle aq metrics fr om v$asm_disk_iostat view.
func ScrapeASM(db *sql.DB, ch chan<- prometheus.Metric) error {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = db.Query("SELECT instname, dbname, group_number, disk_number, failgroup, reads, writes, read_errs, write_errs, read_time, write_time, bytes_read, bytes_written, hot_reads, hot_writes, hot_bytes_read, hot_bytes_written, cold_reads, cold_writes, cold_bytes_read, cold_bytes_written")
	if err != nil {
		return err
	}
	for rows.Next() {
		var instname, dbname, group_number_str, disk_number_str, failgroup_str string
		var group_number, disk_number, failgroup, reads, writes, read_errs, write_errs, read_time, write_time, bytes_read, bytes_written, hot_reads, hot_writes, hot_bytes_read, hot_bytes_written, cold_reads, cold_writes, cold_bytes_read, cold_bytes_written float64
		group_number_str = strconv.FormatFloat(group_number, 'f', -1, 64)
		disk_number_str = strconv.FormatFloat(disk_number, 'f', -1, 64)
		failgroup_str = strconv.FormatFloat(failgroup, 'f', -1, 64)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			reads,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			writes,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			read_errs,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			write_errs,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			read_time,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			write_time,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			bytes_read,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			bytes_written,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			hot_reads,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			hot_writes,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			hot_bytes_read,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			hot_bytes_written,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			cold_reads,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			cold_writes,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			cold_bytes_read,
		)
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName("oracle", "asm", "count"), "Generic counter metric from v$asm_disk_iostat view in Oracle", []string{}, prometheus.Labels{"method": "reads", "instname": instname, "dbname": dbname, "group_number": group_number_str, "disk_number": disk_number_str, "failgroup": failgroup_str}),
			prometheus.CounterValue,
			cold_bytes_written,
		)
	}
	return nil
}

// Oracle gives us some ugly names back. This function cleans things up for Prometheus.
func cleanName(s string) string {
	s = strings.Replace(s, " ", "_", -1) // Remove spaces
	s = strings.Replace(s, "(", "", -1)  // Remove open parenthesis
	s = strings.Replace(s, ")", "", -1)  // Remove close parenthesis
	s = strings.Replace(s, "/", "", -1)  // Remove forward slashes
	s = strings.ToLower(s)
	return s
}

func main() {
	flag.Parse()
	log.Infoln("Starting oracledb_exporter " + Version)
	dsn := os.Getenv("DATA_SOURCE_NAME")
	exporter := NewExporter(dsn)
	prometheus.MustRegister(exporter)
	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(landingPage)
	})
	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
