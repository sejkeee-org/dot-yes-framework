package dotyes

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /fail", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	// паттерн с параметром: в метрики должен попасть шаблон, а не конкретный URL.
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func Test_Middleware_CountsByHandlerAndCode(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(newTestMux()))
	t.Cleanup(srv.Close)

	for range 3 {
		resp, err := http.Get(srv.URL + "/hello")
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	resp, err := http.Get(srv.URL + "/fail")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	out := m.render()
	require.Contains(t, out, `http_requests_total{handler="GET /hello",code="200"} 3`)
	require.Contains(t, out, `http_requests_total{handler="GET /fail",code="500"} 1`)
	require.Contains(t, out, `http_request_duration_seconds_count{handler="GET /hello"} 3`)
}

func Test_Middleware_PatternInsteadOfURL(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(newTestMux()))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/items/42")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	out := m.render()
	require.Contains(t, out, `http_requests_total{handler="GET /items/{id}",code="200"} 1`)
	require.NotContains(t, out, "/items/42")
}

func Test_Middleware_Unmatched(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(newTestMux()))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/nowhere")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	out := m.render()
	require.Contains(t, out, `http_requests_total{handler="unmatched",code="404"} 1`)
}

func Test_Middleware_HistogramBuckets(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(newTestMux()))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/hello")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	out := m.render()
	// последний бакет и +Inf равны общему количеству наблюдений.
	require.Contains(t, out, `http_request_duration_seconds_bucket{handler="GET /hello",le="+Inf"} 1`)
	require.Contains(t, out, `http_request_duration_seconds_count{handler="GET /hello"} 1`)
	// суммарное время неотрицательно и записано числом.
	require.Regexp(t, `http_request_duration_seconds_sum\{handler="GET /hello"\} [0-9.]+`, out)
}

// Метрики пишутся из параллельных запросов без гонок (запуск с -race).
func Test_Middleware_Concurrent(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(newTestMux()))
	t.Cleanup(srv.Close)

	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 10 {
				resp, err := http.Get(srv.URL + "/hello")
				if err == nil {
					_ = resp.Body.Close()
				}
			}
		}()
	}
	for range 8 {
		<-done
	}

	require.Contains(t, m.render(), `http_requests_total{handler="GET /hello",code="200"} 80`)
}

// Middleware остаётся прозрачным: тело и заголовки ответа доходят до клиента.
func Test_Middleware_PassesBodyThrough(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /text", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok"))
	})

	m := NewMetrics()
	srv := httptest.NewServer(m.Middleware(mux))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/text")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(body), "ok"))
}
