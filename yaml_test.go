package dotyes

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_Load_TypedEntries(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
http_addr:
  type: string
  value: ":8080"
pool:
  size:
    type: int
    value: 4
  timeout:
    type: duration
    value: 5s
enabled:
  type: bool
  value: true
ratio:
  type: float
  value: 0.5
deadline:
  type: time
  value: 2026-11-11T12:00:00Z
features:
  type: string
  value: "[a, b]"
`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	entries, err := Load(path)
	require.NoError(t, err)
	// флаги по умолчанию: writable = true, restart_required = false.
	require.Equal(t, map[string]Entry{
		"http_addr":    {Name: "http_addr", Type: TypeString, Value: ":8080", Writable: true},
		"pool.size":    {Name: "pool.size", Type: TypeInt, Value: "4", Writable: true},
		"pool.timeout": {Name: "pool.timeout", Type: TypeDuration, Value: "5s", Writable: true},
		"enabled":      {Name: "enabled", Type: TypeBool, Value: "true", Writable: true},
		"ratio":        {Name: "ratio", Type: TypeFloat, Value: "0.5", Writable: true},
		"deadline":     {Name: "deadline", Type: TypeTime, Value: "2026-11-11T12:00:00Z", Writable: true},
		"features":     {Name: "features", Type: TypeString, Value: "[a, b]", Writable: true},
	}, entries)
}

func Test_Load_Flags(t *testing.T) {
	t.Parallel()

	// флаги читаются из объявления ключа.
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `
http_addr:
  type: string
  value: ":8080"
  writable: false
  restart_required: true
max_players:
  type: int
  value: 4
  writable: true
  restart_required: false
`
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

	entries, err := Load(path)
	require.NoError(t, err)
	require.False(t, entries["http_addr"].Writable)
	require.True(t, entries["http_addr"].RestartRequired)
	require.True(t, entries["max_players"].Writable)
	require.False(t, entries["max_players"].RestartRequired)
}

func Test_Load_BadFlags(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		"key:\n  type: int\n  value: 1\n  writable: yes-please\n",
		"key:\n  type: int\n  value: 1\n  restart_required: maybe\n",
		"key:\n  type: int\n  value: 1\n  extra: x\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

		_, err := Load(path)
		require.Error(t, err, body)
	}
}

func Test_Load_UnknownType(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("key:\n  type: text\n  value: hi\n"), 0o600))

	_, err := Load(path)
	require.Error(t, err)
}

func Test_Load_ValueDoesNotMatchType(t *testing.T) {
	t.Parallel()

	// int с нечисловым значением отбраковывается на старте, а не в рантайме.
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("key:\n  type: int\n  value: fast\n"), 0o600))

	_, err := Load(path)
	require.Error(t, err)
}

func Test_Load_MissingTypeOrValue(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		"key:\n  value: 1\n",
		"key:\n  type: int\n",
		"key:\n  type: int\n  value: 1\n  extra: x\n",
		"key: plain\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

		_, err := Load(path)
		require.Error(t, err, body)
	}
}

func Test_Load_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
}

func Test_ValidType(t *testing.T) {
	t.Parallel()

	for _, typ := range []string{TypeString, TypeInt, TypeFloat, TypeBool, TypeDuration, TypeTime} {
		require.True(t, ValidType(typ), typ)
	}
	require.False(t, ValidType("text"))
	require.False(t, ValidType(""))
}

func Test_ValidateValue(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateValue(TypeInt, "42"))
	require.NoError(t, validateValue(TypeFloat, "0.5"))
	require.NoError(t, validateValue(TypeBool, "true"))
	require.NoError(t, validateValue(TypeDuration, "5s"))
	require.NoError(t, validateValue(TypeTime, "2026-11-11T12:00:00Z"))
	require.NoError(t, validateValue(TypeString, "anything"))

	// строка не проверяется по виду, остальные типы — строго.
	require.Error(t, validateValue(TypeInt, "fast"))
	require.Error(t, validateValue(TypeFloat, "fast"))
	require.Error(t, validateValue(TypeBool, "yes"))
	require.Error(t, validateValue(TypeDuration, "2026-01-01"))
	require.Error(t, validateValue(TypeTime, "2026-01-02"))
}

func Test_DurationGetter(t *testing.T) {
	t.Parallel()

	c := &Client{values: map[string]Entry{"interval": {Type: TypeDuration, Value: "250ms"}}}
	require.Equal(t, 250*time.Millisecond, c.Duration("interval"))
	require.Equal(t, time.Duration(0), c.Duration("missing"))
}

func Test_TimeGetter(t *testing.T) {
	t.Parallel()

	c := &Client{values: map[string]Entry{"deadline": {Type: TypeTime, Value: "2026-11-11T12:00:00Z"}}}
	require.Equal(t, time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC), c.Time("deadline"))
	require.True(t, c.Time("missing").IsZero())
}
