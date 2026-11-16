package dotyes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testApp = "dungeon-master"

// fakeAdmin эмулирует citadel: хранилище ключей, инстансов сервисов и счётчик запросов.
type fakeAdmin struct {
	server   *httptest.Server
	mu       sync.Mutex
	state    map[string]Entry
	services map[string][]instance
	syncs    int
	fails    int
}

func newFakeAdmin(t *testing.T) (*fakeAdmin, *httptest.Server) {
	t.Helper()

	f := &fakeAdmin{state: map[string]Entry{}, services: map[string][]instance{}}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /api/v1/configs/{app}/sync", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.syncs++

		var req syncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// новые ключи дозаписываются, существующие остаются со значениями админки.
		for _, e := range req.Configs {
			if _, ok := f.state[e.Name]; !ok {
				f.state[e.Name] = e
			}
		}
		writeEntries(w, f.state)
	})

	mux.HandleFunc("GET /api/v1/configs/{app}", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.fails > 0 {
			f.fails--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeEntries(w, f.state)
	})

	mux.HandleFunc("PUT /api/v1/discovery/{name}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		var req discoveryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// несколько инстансов одного сервиса: адреса накапливаются.
		name, addr := r.PathValue("name"), req.Addr
		for _, inst := range f.services[name] {
			if inst.Addr == addr {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		f.services[name] = append(f.services[name], instance{Addr: addr, Container: req.Container})
		w.WriteHeader(http.StatusNoContent)
	})

	// инстансы отдаются отсортированными по адресу: как в admin-panel.
	mux.HandleFunc("GET /api/v1/discovery/{name}/instances", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()

		instances, ok := f.services[r.PathValue("name")]
		if !ok || len(instances) == 0 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"not found"}`))
			return
		}
		sort.Slice(instances, func(i, j int) bool { return instances[i].Addr < instances[j].Addr })
		_ = json.NewEncoder(w).Encode(instancesResponse{Instances: instances})
	})

	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f, f.server
}

func (f *fakeAdmin) set(entry Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state[entry.Name] = entry
}

func writeEntries(w http.ResponseWriter, state map[string]Entry) {
	entries := make([]Entry, 0, len(state))
	for _, e := range state {
		entries = append(entries, e)
	}
	_ = json.NewEncoder(w).Encode(syncResponse{Configs: entries})
}

func testClient(t *testing.T, body string, base string) *Client {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	c, err := New(testApp, base, path)
	require.NoError(t, err)
	return c
}

func Test_Sync_RegistersNewKeysAndLoadsValues(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)
	f.set(Entry{Name: "http_addr", Type: TypeString, Value: ":9090"})

	c := testClient(t, "http_addr:\n  type: string\n  value: \":8080\"\nmax_players:\n  type: int\n  value: 4\n", srv.URL)
	require.NoError(t, c.Sync(context.Background()))

	// значение из админки приоритетно, новый ключ дозаписан и загружен обратно.
	require.Equal(t, ":9090", c.String("http_addr"))
	require.Equal(t, 4, c.Int("max_players"))
	require.GreaterOrEqual(t, f.syncs, 1)
}

func Test_Sync_FallbackToLocalOnFailure(t *testing.T) {
	t.Parallel()

	// несуществующий адрес админки: приложение продолжает работать на yaml-значениях.
	c := testClient(t, "http_addr:\n  type: string\n  value: \":8080\"\n", "http://127.0.0.1:1")
	require.Error(t, c.Sync(context.Background()))
	require.Equal(t, ":8080", c.String("http_addr"))
}

func Test_Watch_AppliesUpdatesAndNotifies(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)

	c := testClient(t, "poll_interval:\n  type: duration\n  value: 1s\n", srv.URL)
	require.NoError(t, c.Sync(context.Background()))

	changed := make(chan []Entry, 1)
	c.OnChange(func(entries []Entry) { changed <- entries })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Watch(ctx, 20*time.Millisecond)

	// админ меняет значение — библиотека подхватывает его без перезапуска.
	f.set(Entry{Name: "poll_interval", Type: TypeDuration, Value: "3s"})

	select {
	case entries := <-changed:
		require.Len(t, entries, 1)
		require.Equal(t, Entry{Name: "poll_interval", Type: TypeDuration, Value: "3s"}, entries[0])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for config update")
	}
	require.Equal(t, "3s", c.String("poll_interval"))
}

func Test_Watch_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)
	require.NoError(t, c.Sync(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Watch(ctx, 20*time.Millisecond)
		close(done)
	}()

	f.mu.Lock()
	f.fails = 1
	f.mu.Unlock()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not stop")
	}
}

func Test_Watch_SurvivesPollErrors(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)
	require.NoError(t, c.Sync(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Watch(ctx, 20*time.Millisecond)

	// ошибки опроса не завершают наблюдение: следующая попытка успешна.
	f.set(Entry{Name: "key", Type: TypeInt, Value: "2"})
	require.Eventually(t, func() bool { return c.Int("key") == 2 }, 2*time.Second, 10*time.Millisecond)
}

func Test_Getters(t *testing.T) {
	t.Parallel()

	c := &Client{values: map[string]Entry{
		"s": {Type: TypeString, Value: "hello"},
		"i": {Type: TypeInt, Value: "42"},
		"b": {Type: TypeBool, Value: "true"},
		"f": {Type: TypeFloat, Value: "0.25"},
		"d": {Type: TypeDuration, Value: "1m"},
	}}
	require.Equal(t, "hello", c.String("s"))
	require.Equal(t, 42, c.Int("i"))
	require.True(t, c.Bool("b"))
	require.InDelta(t, 0.25, c.Float("f"), 1e-9)
	require.Equal(t, time.Minute, c.Duration("d"))

	// отсутствующие и нестандартные значения дают нулевые.
	require.Empty(t, c.String("missing"))
	require.Zero(t, c.Int("missing"))
	require.False(t, c.Bool("missing"))
	require.Zero(t, c.Float("missing"))
	require.Zero(t, c.Duration("missing"))
	require.Zero(t, c.Int("s"))
}

func Test_New_MissingYAML(t *testing.T) {
	t.Parallel()

	_, err := New(testApp, "http://localhost:18081", filepath.Join(t.TempDir(), "missing.yaml"))
	require.True(t, errors.Is(err, os.ErrNotExist))
}

func Test_Register_Discover(t *testing.T) {
	t.Parallel()

	_, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app:8080"))

	addr, err := c.Discover(context.Background(), "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, "app:8080", addr)

	// незарегистрированный сервис — ErrNotFound.
	_, err = c.Discover(context.Background(), "unknown")
	require.True(t, errors.Is(err, ErrNotFound))
}

func Test_DiscoverAll_MultipleInstances(t *testing.T) {
	t.Parallel()

	_, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	// один сервис, два инстанса.
	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app-1:8080"))
	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app-2:8080"))

	addrs, err := c.DiscoverAll(context.Background(), "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, []string{"app-1:8080", "app-2:8080"}, addrs)
}

func Test_DiscoverAll_CachesUntilInvalidate(t *testing.T) {
	t.Parallel()

	_, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app-1:8080"))

	ctx := context.Background()
	addrs, err := c.DiscoverAll(ctx, "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, []string{"app-1:8080"}, addrs)

	// новый инстанс зарегистрирован, но кэш продолжает отдавать старый список.
	require.NoError(t, c.Register(ctx, "dungeon-master", "app-2:8080"))
	addrs, err = c.DiscoverAll(ctx, "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, []string{"app-1:8080"}, addrs)

	// отчёт о несуществующем инстансе сбрасывает кэш: список запрашивается заново.
	c.Invalidate("dungeon-master")
	addrs, err = c.DiscoverAll(ctx, "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, []string{"app-1:8080", "app-2:8080"}, addrs)
}

func Test_Discover_RoundRobin(t *testing.T) {
	t.Parallel()

	_, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app-1:8080"))
	require.NoError(t, c.Register(context.Background(), "dungeon-master", "app-2:8080"))

	// адреса выбираются по round-robin в порядке сортировки.
	ctx := context.Background()
	got := make([]string, 0, 4)
	for range 4 {
		addr, err := c.Discover(ctx, "dungeon-master")
		require.NoError(t, err)
		got = append(got, addr)
	}
	require.Equal(t, []string{"app-1:8080", "app-2:8080", "app-1:8080", "app-2:8080"}, got)
}

func Test_DiscoverAll_SameHost_UsesContainerName(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	// клиент живёт на хосте 10.0.0.1: локальный инстанс резолвится по имени
	// контейнера, соседний по хосту — по обычному адресу.
	require.NoError(t, c.Register(context.Background(), "admin-panel", "10.0.0.1:18081"))
	f.mu.Lock()
	f.services["dungeon-master"] = []instance{
		{Addr: "10.0.0.1:18080", Container: "dungeon-master-0"},
		{Addr: "10.0.0.2:18080", Container: "dungeon-master-0"},
	}
	f.mu.Unlock()

	addrs, err := c.DiscoverAll(context.Background(), "dungeon-master")
	require.NoError(t, err)
	require.Equal(t, []string{"dungeon-master-0:80", "10.0.0.2:18080"}, addrs)
}

func Test_Register_SendsContainerName(t *testing.T) {
	t.Setenv(containerNameEnv, "admin-panel-0")

	f, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	require.NoError(t, c.Register(context.Background(), "admin-panel", "10.0.0.1:18081"))

	f.mu.Lock()
	defer f.mu.Unlock()
	require.Equal(t, []instance{{Addr: "10.0.0.1:18081", Container: "admin-panel-0"}}, f.services["admin-panel"])
}

func Test_KeepAlive_RenewsRegistration(t *testing.T) {
	t.Parallel()

	f, srv := newFakeAdmin(t)
	c := testClient(t, "key:\n  type: int\n  value: 1\n", srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.KeepAlive(ctx, "dungeon-master", "app:8080", func() time.Duration { return 40 * time.Millisecond })

	// админка видит свежую регистрацию, пока KeepAlive жив.
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.services["dungeon-master"]) == 1 && f.services["dungeon-master"][0].Addr == "app:8080"
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
}

func Test_ParseCitadel(t *testing.T) {
	t.Parallel()

	citadels, err := parseCitadel("http://10.10.0.1:18082=citadel-0, 10.10.0.2:18082=citadel-1, localhost:18082")
	require.NoError(t, err)
	require.Equal(t, []instance{
		{Addr: "10.10.0.1:18082", Container: "citadel-0"},
		{Addr: "10.10.0.2:18082", Container: "citadel-1"},
		{Addr: "localhost:18082"},
	}, citadels)

	_, err = parseCitadel("bad-addr")
	require.Error(t, err)

	_, err = parseCitadel(" , ")
	require.Error(t, err)
}

func Test_Do_FailoverToNextCitadel(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("key:\n  type: int\n  value: 1\n"), 0o600))

	// первый инстанс недоступен: запрос уходит во второй и succeeds.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(syncResponse{Configs: []Entry{{Name: "key", Type: TypeString, Value: "2"}}})
	}))
	t.Cleanup(live.Close)

	c, err := New(testApp, deadURL+","+live.URL, path)
	require.NoError(t, err)
	require.NoError(t, c.Sync(context.Background()))
	require.Equal(t, "2", c.String("key"))
}

func Test_CitadelAddr_PrefersSameHost(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("key:\n  type: int\n  value: 1\n"), 0o600))

	// сервис на 10.10.0.1: все запросы к citadel идут в локальный инстанс
	// по имени контейнера, а не по round-robin на чужой сервер. selfHost
	// заполняется в Register; здесь он задан напрямую, без сети.
	c, err := New(testApp, "10.10.0.1:18082=citadel-0,10.10.0.2:18082=citadel-1", path)
	require.NoError(t, err)
	c.selfHost.Store("10.10.0.1")

	for range 3 {
		require.Equal(t, "http://citadel-0:80", c.CitadelAddr())
	}

	// до регистрации локальный хост неизвестен: round-robin по публичным адресам.
	c2, err := New(testApp, "10.10.0.1:18082=citadel-0,10.10.0.2:18082=citadel-1", path)
	require.NoError(t, err)
	require.Equal(t, "http://10.10.0.1:18082", c2.CitadelAddr())
	require.Equal(t, "http://10.10.0.2:18082", c2.CitadelAddr())
}
