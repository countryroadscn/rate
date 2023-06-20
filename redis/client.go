package rate

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// ConfigRedis sets the Redis.
type ConfigRedis struct {
	Addrs       []string `yaml:"addrs"` // host:port
	Password    string   `yaml:"password"`
	IdleTimeout int      `yaml:"idle_timeout"`
}

// newRedisClient constructs a redis client.
func newRedisClient(config ConfigRedis) *redis.Ring {
	addrs := make(map[string]string)
	for _, addr := range config.Addrs {
		addrs[addr] = addr
	}
	client := redis.NewRing(&redis.RingOptions{
		Addrs:    addrs,
		Password: config.Password,
	})

	err := checkRedisClient(client)
	if err != nil {
		return nil
	}
	return client
}

// check redis client connection.
func checkRedisClient(client *redis.Ring) error {
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		log.Println("fail to ping redis client: ", err)
		return err
	}
	return nil
}
