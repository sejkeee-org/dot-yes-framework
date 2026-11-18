package dotyes

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// floatBits разрядность для разбора и печати вещественных значений.
const floatBits = 64

// Load читает yaml-конфиг и возвращает плоский набор ключей. Каждый ключ
// объявляет свой тип и значение; опционально — writable (можно ли менять
// значение в админке, по умолчанию да) и restart_required (нужен ли рестарт
// для применения, по умолчанию нет):
//
//	http_addr:
//	  type: string
//	  value: ":8080"
//	  writable: false
//	  restart_required: true
//	max_players:
//	  type: int
//	  value: 4
//
// Вложенные секции сплющиваются через точку (pool.timeout); значение —
// скаляр, сложные значения задаются JSON-строкой.
func Load(path string) (map[string]Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed read config %q: %w", path, err)
	}

	var raw map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed parse config %q: %w", path, err)
	}

	out := make(map[string]Entry)
	if err := flatten("", raw, out); err != nil {
		return nil, fmt.Errorf("failed load config %q: %w", path, err)
	}
	return out, nil
}

// flatten рекурсивно сплющивает yaml-секции в ключи вида section.field.
func flatten(prefix string, in map[string]any, out map[string]Entry) error {
	for key, val := range in {
		name := key
		if prefix != "" {
			name = prefix + "." + key
		}
		if section, ok := val.(map[string]any); ok && !isEntry(section) {
			if err := flatten(name, section, out); err != nil {
				return err
			}
			continue
		}
		e, err := toEntry(name, val)
		if err != nil {
			return err
		}
		out[name] = e
	}
	return nil
}

// isEntry проверяет, что секция — объявление ключа: обязательны type и value,
// опциональны writable и restart_required.
func isEntry(section map[string]any) bool {
	_, hasType := section["type"]
	_, hasValue := section["value"]
	return hasType && hasValue
}

// toEntry собирает ключ из объявления type/value, проверяя тип и значение.
func toEntry(name string, val any) (Entry, error) {
	section, ok := val.(map[string]any)
	if !ok {
		return Entry{}, fmt.Errorf("key %q must define type and value", name)
	}
	typ, _ := section["type"].(string)
	if !ValidType(typ) {
		return Entry{}, fmt.Errorf("key %q has unknown type %q", name, typ)
	}
	value := stringify(section["value"])
	if err := validateValue(typ, value); err != nil {
		return Entry{}, fmt.Errorf("key %q: %w", name, err)
	}
	// по умолчанию ключ доступен для изменения и применяется без рестарта.
	e := Entry{Name: name, Type: typ, Value: value, Writable: true}
	for key, v := range section {
		switch key {
		case "type", "value":
		case "writable":
			b, ok := v.(bool)
			if !ok {
				return Entry{}, fmt.Errorf("key %q: writable must be boolean", name)
			}
			e.Writable = b
		case "restart_required":
			b, ok := v.(bool)
			if !ok {
				return Entry{}, fmt.Errorf("key %q: restart_required must be boolean", name)
			}
			e.RestartRequired = b
		default:
			return Entry{}, fmt.Errorf("key %q: unknown field %q", name, key)
		}
	}
	return e, nil
}

// ValidType проверяет, что тип поддерживается.
func ValidType(typ string) bool {
	switch typ {
	case TypeString, TypeInt, TypeFloat, TypeBool, TypeDuration, TypeTime:
		return true
	default:
		return false
	}
}

// validateValue проверяет, что значение разбирается как заявленный тип.
func validateValue(typ, value string) error {
	var err error
	switch typ {
	case TypeInt:
		_, err = strconv.Atoi(value)
	case TypeFloat:
		_, err = strconv.ParseFloat(value, floatBits)
	case TypeBool:
		_, err = strconv.ParseBool(value)
	case TypeDuration:
		_, err = time.ParseDuration(value)
	case TypeTime:
		_, err = time.Parse(time.RFC3339, value)
	}
	if err != nil {
		return fmt.Errorf("value %q does not match type %q: %w", value, typ, err)
	}
	return nil
}

// stringify приводит значение yaml к строке: списки и объекты — JSON, остальное — текст.
func stringify(val any) string {
	switch v := val.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, floatBits)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(data)
	}
}
