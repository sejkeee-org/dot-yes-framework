package dotyes

import (
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// defaultBuckets бакеты гистограммы времени ответа, секунды.
var defaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Имена метрик и метка для запросов вне зарегистрированных маршрутов.
const (
	requestsMetric   = "http_requests_total"
	durationMetric   = "http_request_duration_seconds"
	unmatchedHandler = "unmatched"
)

type (
	// Metrics метрики HTTP-сервера в формате Prometheus: количество запросов
	// по хендлерам и кодам ответа (RPS и доли кодов) и гистограмма времени ответа.
	Metrics struct {
		mu     sync.Mutex
		counts map[string]map[string]uint64 // хендлер → код ответа → количество
		histos map[string]*histogram
	}

	// histogram гистограмма времени ответа одного хендлера.
	histogram struct {
		counts []uint64 // попадания в бакеты, counts[i] включает counts[i-1]
		sum    float64
		total  uint64
	}
)

// NewMetrics конструктор метрик HTTP-сервера.
func NewMetrics() *Metrics {
	return &Metrics{
		counts: make(map[string]map[string]uint64),
		histos: make(map[string]*histogram),
	}
}

// Handler отдаёт метрики в текстовом формате Prometheus; монтируется как GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if _, err := w.Write([]byte(m.render())); err != nil {
			slog.Error("failed write metrics", slog.Any("error", err))
		}
	})
}

// observe фиксирует один обработанный запрос.
func (m *Metrics) observe(handler string, code int, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	byCode, ok := m.counts[handler]
	if !ok {
		byCode = make(map[string]uint64)
		m.counts[handler] = byCode
	}
	byCode[strconv.Itoa(code)]++

	h, ok := m.histos[handler]
	if !ok {
		h = &histogram{counts: make([]uint64, len(defaultBuckets))}
		m.histos[handler] = h
	}
	h.observe(seconds)
}

// observe добавляет наблюдение в гистограмму.
func (h *histogram) observe(v float64) {
	for i, bound := range defaultBuckets {
		if v <= bound {
			h.counts[i]++
		}
	}
	h.sum += v
	h.total++
}

// render собирает срез метрик в текстовом формате Prometheus; детерминированный
// порядок строк — хендлеры и коды отсортированы.
func (m *Metrics) render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	handlers := make([]string, 0, len(m.counts))
	for h := range m.counts {
		handlers = append(handlers, h)
	}
	sort.Strings(handlers)

	var b strings.Builder
	b.WriteString("# HELP " + requestsMetric + " Количество обработанных HTTP-запросов.\n")
	b.WriteString("# TYPE " + requestsMetric + " counter\n")
	for _, h := range handlers {
		codes := make([]string, 0, len(m.counts[h]))
		for c := range m.counts[h] {
			codes = append(codes, c)
		}
		sort.Strings(codes)
		for _, c := range codes {
			b.WriteString(requestsMetric + `{handler="` + h + `",code="` + c + `"} ` + strconv.FormatUint(m.counts[h][c], 10) + "\n")
		}
	}

	b.WriteString("# HELP " + durationMetric + " Время обработки HTTP-запроса, секунды.\n")
	b.WriteString("# TYPE " + durationMetric + " histogram\n")
	for _, h := range handlers {
		hist, ok := m.histos[h]
		if !ok {
			continue
		}
		for i, bound := range defaultBuckets {
			b.WriteString(durationMetric + `_bucket{handler="` + h + `",le="` + formatFloat(bound) + `"} ` + strconv.FormatUint(hist.counts[i], 10) + "\n")
		}
		b.WriteString(durationMetric + `_bucket{handler="` + h + `",le="+Inf"} ` + strconv.FormatUint(hist.total, 10) + "\n")
		b.WriteString(durationMetric + `_sum{handler="` + h + `"} ` + formatFloat(hist.sum) + "\n")
		b.WriteString(durationMetric + `_count{handler="` + h + `"} ` + strconv.FormatUint(hist.total, 10) + "\n")
	}
	return b.String()
}

// formatFloat печатает число без лишних знаков.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
