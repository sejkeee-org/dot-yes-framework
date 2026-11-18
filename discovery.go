// Discovery-часть фреймворка: регистрация сервиса в citadel и поиск
// по имени с клиентской балансировкой между живыми инстансами.
package dotyes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

const (
	discoveryPath          = "/api/v1/discovery/%s"
	discoveryInstancesPath = "/api/v1/discovery/%s/instances"
)

type (
	discoveryRequest struct {
		Addr string `json:"addr"`
	}
	// instance инстанс из discovery: адрес вида «хост:порт».
	instance struct {
		Addr string `json:"addr"`
	}
	instancesResponse struct {
		Instances []instance `json:"instances"`
	}
)

// Register регистрирует инстанс сервиса в citadel: несколько инстансов
// одного сервиса регистрируются под одним именем с разными адресами.
func (c *Client) Register(ctx context.Context, name, addr string) error {
	body, err := json.Marshal(discoveryRequest{Addr: addr})
	if err != nil {
		return fmt.Errorf("failed marshal request: %w", err)
	}
	if _, err := c.do(ctx, http.MethodPut, discoveryPath, body, http.StatusNoContent, name); err != nil {
		return fmt.Errorf("failed register service: %w", err)
	}
	return nil
}

// DiscoverAll возвращает адреса всех живых инстансов сервиса; ErrNotFound,
// если сервис не зарегистрирован или TTL всех инстансов истёк. Адреса
// кэшируются: citadel опрашивается, пока запрос по кэшированному адресу
// не уйдёт в несуществующий инстанс (сброс кэша — Invalidate).
func (c *Client) DiscoverAll(ctx context.Context, name string) ([]string, error) {
	if cached, ok := c.discovered.Load(name); ok {
		return cached.([]string), nil
	}

	data, err := c.do(ctx, http.MethodGet, discoveryInstancesPath, nil, http.StatusOK, name)
	if err != nil {
		return nil, fmt.Errorf("failed discover service: %w", err)
	}

	var resp instancesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("failed decode response: %w", err)
	}
	if len(resp.Instances) == 0 {
		return nil, ErrNotFound
	}

	addrs := make([]string, 0, len(resp.Instances))
	for _, inst := range resp.Instances {
		addrs = append(addrs, "http://"+inst.Addr)
	}

	c.discovered.Store(name, addrs)
	return addrs, nil
}

// Invalidate сбрасывает кэш адресов сервиса: следующий Discover заново
// спросит citadel. Вызывается, когда запрос по адресу из discovery ушёл
// в несуществующий инстанс.
func (c *Client) Invalidate(name string) {
	c.discovered.Delete(name)
}

// CitadelAddr возвращает очередной инстанс citadel по round-robin.
// Для потребителей, ходящих в citadel напрямую (reverse proxy панели).
func (c *Client) CitadelAddr() string {
	start := int(c.citadelIdx.Add(1) - 1)
	inst := c.citadels[start%len(c.citadels)]
	return "http://" + inst.Addr
}

// Discover возвращает адрес одного живого инстанса, выбирая их по round-robin;
// ErrNotFound, если живых инстансов нет.
func (c *Client) Discover(ctx context.Context, name string) (string, error) {
	addrs, err := c.DiscoverAll(ctx, name)
	if err != nil {
		return "", err
	}
	return c.pick(name, addrs), nil
}

// KeepAlive перерегистрирует инстанс, пока жив контекст: citadel считает
// инстанс живым, пока приходят регистрации. ttl выдаёт актуальное время жизни
// регистрации: интервал перерегистрации следует за его изменениями.
func (c *Client) KeepAlive(ctx context.Context, name, addr string, ttl func() time.Duration) {
	for {
		interval := ttl() / 2
		if interval <= 0 {
			interval = defaultWatchInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		// ошибка перерегистрации уже залогирована в do; пропускаем такт.
		_ = c.Register(ctx, name, addr)
	}
}

// pick выбирает адрес инстанса по round-robin: у каждого имени свой счётчик.
func (c *Client) pick(name string, addrs []string) string {
	p, _ := c.pickers.LoadOrStore(name, new(uint64))
	n := atomic.AddUint64(p.(*uint64), 1) - 1
	return addrs[n%uint64(len(addrs))]
}
