// Package dotyes — фреймворк для сервисов: realtime-конфиг из citadel
// (yaml дозаписывает новые ключи, изменения подхватываются без перезапуска),
// service discovery с клиентской балансировкой между инстансами и HTTP-мидлвара
// с метриками Prometheus (время ответа, коды, RPS по хендлерам).
package dotyes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotFound — сервис не зарегистрирован в citadel или его TTL истёк.
var ErrNotFound = errors.New("service not found")

const (
	httpTimeout          = 3 * time.Second
	defaultWatchInterval = 5 * time.Second
)

const (
	syncPath  = "/api/v1/configs/%s/sync"
	statePath = "/api/v1/configs/%s"
)

// Entry ключ конфига приложения: имя, тип и строковое значение.
type Entry struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type (
	syncRequest struct {
		Configs []Entry `json:"configs"`
	}
	syncResponse struct {
		Configs []Entry `json:"configs"`
	}
)

// Client конфиг приложения: до синхронизации — значения из yaml,
// после — значения из citadel, обновляемые в реальном времени.
type Client struct {
	app  string
	base string
	http *http.Client

	mu       sync.RWMutex
	values   map[string]Entry
	onChange func([]Entry)

	// pickers: имя сервиса → счётчик round-robin для балансировки инстансов.
	pickers sync.Map
	// discovered: кэш адресов discovery; живёт, пока запросы по кэшированным
	// адресам доходят до существующих инстансов, сбрасывается Invalidate.
	discovered sync.Map
	// selfHost хост собственного advertise-адреса: инстансы того же хоста
	// резолвятся по имени контейнера (docker-сеть). Заполняется в Register.
	selfHost atomic.Value
}

// New читает yaml-конфиг приложения и возвращает клиент citadel.
func New(app, adminURL, yamlPath string) (*Client, error) {
	local, err := Load(yamlPath)
	if err != nil {
		return nil, err
	}
	return &Client{
		app:    app,
		base:   strings.TrimRight(adminURL, "/"),
		http:   &http.Client{Timeout: httpTimeout},
		values: local,
	}, nil
}

// Sync отправляет в citadel ключи из yaml: отсутствующие дозаписываются,
// существующие остаются со значениями из админки. Загружает актуальные значения.
func (c *Client) Sync(ctx context.Context) error {
	c.mu.RLock()
	req := syncRequest{Configs: make([]Entry, 0, len(c.values))}
	for _, e := range c.values {
		req.Configs = append(req.Configs, e)
	}
	c.mu.RUnlock()
	sort.Slice(req.Configs, func(i, j int) bool { return req.Configs[i].Name < req.Configs[j].Name })

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed marshal request: %w", err)
	}
	data, err := c.do(ctx, http.MethodPost, syncPath, body, http.StatusOK)
	if err != nil {
		return fmt.Errorf("failed sync config: %w", err)
	}

	var resp syncResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("failed decode response: %w", err)
	}

	values := make(map[string]Entry, len(resp.Configs))
	for _, e := range resp.Configs {
		values[e.Name] = e
	}
	c.mu.Lock()
	c.values = values
	c.mu.Unlock()
	return nil
}

// OnChange подписка на обновления значений; вызывается из Watch.
func (c *Client) OnChange(fn func(changed []Entry)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onChange = fn
}

// Watch опрашивает citadel и обновляет значения без перезапуска
// приложения; завершается по отмене контекста. Запускать после Sync.
func (c *Client) Watch(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = defaultWatchInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		data, err := c.do(ctx, http.MethodGet, statePath, nil, http.StatusOK)
		if err != nil {
			continue
		}
		var resp syncResponse
		if err := json.Unmarshal(data, &resp); err != nil {
			slog.Warn("admin config poll decode failed", slog.Any("error", err))
			continue
		}
		c.apply(resp.Configs)
	}
}

// apply сохраняет новые значения и уведомляет подписчика об изменившихся ключах.
func (c *Client) apply(entries []Entry) {
	changed := make([]Entry, 0, len(entries))

	c.mu.Lock()
	for _, e := range entries {
		if old, ok := c.values[e.Name]; !ok || old != e {
			changed = append(changed, e)
		}
		c.values[e.Name] = e
	}
	fn := c.onChange
	c.mu.Unlock()

	if len(changed) > 0 && fn != nil {
		fn(changed)
	}
}

// Value возвращает актуальное значение ключа в строковом виде.
func (c *Client) Value(name string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[name].Value
}

// String возвращает значение ключа строкой.
func (c *Client) String(name string) string {
	return c.Value(name)
}

// Int возвращает значение ключа как int; 0, если значения нет или оно не число.
func (c *Client) Int(name string) int {
	v, _ := strconv.Atoi(c.Value(name))
	return v
}

// Bool возвращает значение ключа как bool; false, если значения нет или оно не булево.
func (c *Client) Bool(name string) bool {
	v, _ := strconv.ParseBool(c.Value(name))
	return v
}

// Float возвращает значение ключа как float64; 0, если значения нет или оно не число.
func (c *Client) Float(name string) float64 {
	v, _ := strconv.ParseFloat(c.Value(name), floatBits)
	return v
}

// Duration возвращает значение ключа как time.Duration; 0, если значения нет или оно не длительность.
func (c *Client) Duration(name string) time.Duration {
	v, _ := time.ParseDuration(c.Value(name))
	return v
}

// Time возвращает значение ключа как time.Time; нулевое, если значения нет или оно не RFC 3339.
func (c *Client) Time(name string) time.Time {
	v, _ := time.Parse(time.RFC3339, c.Value(name))
	return v
}

// do выполняет запрос к citadel и возвращает тело ответа; каждая ошибка
// вызова логируется. pathArgs — подстановки в path (по умолчанию имя приложения).
func (c *Client) do(ctx context.Context, method, path string, body []byte, wantStatus int, pathArgs ...string) ([]byte, error) {
	args := make([]any, 0, len(pathArgs)+1)
	for _, arg := range pathArgs {
		args = append(args, arg)
	}
	if len(args) == 0 {
		args = append(args, c.app)
	}
	url := c.base + fmt.Sprintf(path, args...)

	data, err := c.send(ctx, method, url, body, wantStatus)
	if err != nil {
		slog.Error("citadel request failed",
			slog.String("method", method),
			slog.String("url", url),
			slog.Any("error", err))
	}
	return data, err
}

// send выполняет HTTP-запрос и возвращает тело ответа.
func (c *Client) send(ctx context.Context, method, url string, body []byte, wantStatus int) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, fmt.Errorf("failed build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed read response: %w", err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrNotFound
	case resp.StatusCode != wantStatus:
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, data)
	}
	return data, nil
}
