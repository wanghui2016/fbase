package util

//json cfg file utility
import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	data map[string]interface{}
	Raw  []byte
}

func NewConfig(path string) (cfg *Config, e error) {
	cfg = new(Config)
	cfg.data = make(map[string]interface{})
	e = cfg.parse(path)
	if e != nil {
		cfg = nil
	}
	return
}

func NewEmptyConfig() (cfg *Config) {
	cfg = new(Config)
	cfg.data = make(map[string]interface{})
	return
}

func (c *Config) Set(key string, value interface{}) {
	c.data[key] = value
}

func (c *Config) GetString(key string) string {
	result, present := c.data[key]
	if !present {
		return ""
	}
	return result.(string)
}

func (c *Config) parse(fileName string) error {
	jsonFileBytes, err := ioutil.ReadFile(fileName)
	c.Raw = jsonFileBytes
	if err == nil {
		err = json.Unmarshal(jsonFileBytes, &c.data)
	}
	return err
}

func (c *Config) GetFloat(key string) float64 {
	x, ok := c.data[key]
	if !ok {
		return -1
	}
	return x.(float64)
}

func (c *Config) GetBool(key string) bool {
	x, ok := c.data[key]
	if !ok {
		return false
	}
	return x.(bool)
}

func (c *Config) GetArray(key string) []interface{} {
	result, present := c.data[key]
	if !present {
		return []interface{}(nil)
	}
	return result.([]interface{})
}

func (c *Config) GetInt(key string) int64 {
	x, ok := c.data[key]
	if !ok {
		return -1
	}
	return x.(int64)
}
