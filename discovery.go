// Discovery-часть фреймворка: регистрация сервиса в citadel и поиск
// по имени с клиентской балансировкой между живыми инстансами.
package dotyes

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

const (
	discoveryPath          = "/api/v1/discovery/%s"
	discoveryInstancesPath = "/api/v1/discovery/%s/instances"

	// containerNameEnv переменная окружения с именем docker-контейнера;
	// задаётся при деплое, пустое значение — локальный запуск без docker-сети.
	containerNameEnv = "CONTAINER_NAME"
	// containerPort порт внутри контейнера: все сервисы слушают :80,
	// наружу инстанс маппится на хост-порт из advertise-адреса.
	containerPort = "80"
)

// containerName возвращает имя docker-контейнера из окружения.
func containerName() string {
	return os.Getenv(containerNameEnv)
}

// selfHostAddr возвращает хост собственного advertise-адреса; пустую строку,
// если сервис ещё не регистрировался.
func (c *Client) selfHostAddr() string {
	host, _ := c.selfHost.Load().(string)
	return host
}

type (
	discoveryRequest struct {
		Addr      string `json:"addr"`
		Container string `json:"container,omitempty"`
	}
	// instance инстанс из discovery: адрес и имя контейнера.
	instance struct {
		Addr      string `json:"addr"`
		Container string `json:"container"`
	}
	instancesResponse struct {
		Instances []instance `json:"instances"`
	}
)

// Register регистрирует инстанс сервиса в citadel: несколько инстансов
// одного сервиса регистрируются под одним именем с разными адресами.
// Вместе с адресом регистрируется имя контейнера из окружения: по нему
// инстансы одного хоста ходят друг к другу по docker-сети.
func (c *Client) Register(ctx context.Context, name, addr string) error {
	req := discoveryRequest{Addr: addr, Container: containerName()}
	// свой хост запоминаем: он определяет, какие инстансы считаются локальными.
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		c.selfHost.Store(host)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed marshal request: %w", err)
	}
	if _, err := c.do(ctx, http.MethodPut, discoveryPath, body, http.StatusNoContent, name); err != nil {
		return fmt.Errorf("failed register service: %w", err)
	}
	return nil
}

// DiscoverAll возвращает адреса всех живых инстансов сервиса; ErrNotFound,
// если сервис не зарегистрирован или TTL всех инстансов истёк. Адрес инстанса
// того же хоста, что и сам сервис, заменяется на адрес по имени контейнера:
// внутри docker-сети ходят напрямую, без хост-порта. Адреса кэшируются:
// citadel опрашивается, пока запрос по кэшированному адресу не уйдёт
// в несуществующий инстанс (сброс кэша — Invalidate).
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
	self := c.selfHostAddr()
	for _, inst := range resp.Instances {
		addrs = append(addrs, localAddr(inst, self))
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

// CitadelAddr возвращает адрес инстанса citadel. Если инстанс живёт на том же
// хосте, что и сам сервис, — адрес по имени его контейнера (docker-сеть);
// иначе очередной инстанс по round-robin. Для потребителей, ходящих в citadel
// напрямую (reverse proxy контрольной панели).
func (c *Client) CitadelAddr() string {
	self := c.selfHostAddr()
	if self != "" {
		for _, inst := range c.citadels {
			if host, _, err := net.SplitHostPort(inst.Addr); err == nil && host == self {
				return "http://" + localAddr(inst, self)
			}
		}
	}
	start := int(c.citadelIdx.Add(1) - 1)
	return "http://" + localAddr(c.citadels[start%len(c.citadels)], self)
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

// localAddr переписывает адрес инстанса того же хоста на адрес по имени
// контейнера: контейнеры одного сервера ходят по docker-сети напрямую.
func localAddr(inst instance, selfHost string) string {
	if inst.Container == "" || selfHost == "" {
		return inst.Addr
	}
	host, _, err := net.SplitHostPort(inst.Addr)
	if err != nil || host != selfHost {
		return inst.Addr
	}
	return net.JoinHostPort(inst.Container, containerPort)
}

// sameHost проверяет, что инстанс зарегистрирован на хосте selfHost.
func sameHost(inst instance, selfHost string) bool {
	if selfHost == "" {
		return false
	}
	host, _, err := net.SplitHostPort(inst.Addr)
	return err == nil && host == selfHost
}

// citadelOrder выстраивает порядок обхода инстансов citadel: локальные
// (тот же хост) первыми — по docker-сети, остальные начиная с очередного
// по round-robin — как фейловер.
func (c *Client) citadelOrder(start int) []instance {
	self := c.selfHostAddr()
	order := make([]instance, 0, len(c.citadels))
	for _, inst := range c.citadels {
		if sameHost(inst, self) {
			order = append(order, inst)
		}
	}
	for i := range c.citadels {
		inst := c.citadels[(start+i)%len(c.citadels)]
		if !sameHost(inst, self) {
			order = append(order, inst)
		}
	}
	return order
}
