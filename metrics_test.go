package dotyes

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Handler_ServesPrometheusText(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.observe("/api", 200, 0.01)

	srv := httptest.NewServer(m.Handler())
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
}

// Срез метрик отсортирован: одинаковый ввод даёт одинаковый вывод.
func Test_Render_Deterministic(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.observe("/b", 500, 0.1)
	m.observe("/a", 200, 0.01)
	m.observe("/a", 404, 0.02)

	first := m.render()
	second := m.render()
	require.Equal(t, first, second)

	// хендлеры идут в алфавитном порядке.
	require.Less(t,
		strings.Index(first, `handler="/a"`),
		strings.Index(first, `handler="/b"`),
	)
	require.Contains(t, first, `http_requests_total{handler="/a",code="200"} 1`)
	require.Contains(t, first, `http_requests_total{handler="/a",code="404"} 1`)
	require.Contains(t, first, `http_requests_total{handler="/b",code="500"} 1`)
}

// Гистограмма: кумулятивные бакеты и суммы растут согласованно.
func Test_Render_HistogramCumulative(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.observe("/api", 200, 0.02) // попадает в le=0.025 и дальше
	m.observe("/api", 200, 0.3)  // попадает в le=0.5 и дальше

	out := m.render()
	require.Contains(t, out, `http_request_duration_seconds_bucket{handler="/api",le="0.025"} 1`)
	require.Contains(t, out, `http_request_duration_seconds_bucket{handler="/api",le="0.5"} 2`)
	require.Contains(t, out, `http_request_duration_seconds_bucket{handler="/api",le="+Inf"} 2`)
	require.Contains(t, out, `http_request_duration_seconds_count{handler="/api"} 2`)
}
