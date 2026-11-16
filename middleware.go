package dotyes

import (
	"net/http"
	"time"
)

// Middleware оборачивает mux метриками: по паттерну совпавшего маршрута
// (низкая кардинальность вместо конкретных URL) фиксируются код ответа
// и время обработки. Запросы вне маршрутов получают метку "unmatched".
func (m *Metrics) Middleware(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			pattern = unmatchedHandler
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		mux.ServeHTTP(rec, r)
		m.observe(pattern, rec.code, time.Since(start).Seconds())
	})
}

// statusRecorder запоминает код ответа, оставаясь прозрачным для хендлера.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}
