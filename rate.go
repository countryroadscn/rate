// Package redis provides a rate limiter based on redis.
package rate

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	scriptHash  string
)

var allowN = redis.NewScript(`
-- this script has side-effects, so it requires replicate commands mode
redis.replicate_commands()

local rate_limit_key = KEYS[1]
local burst = ARGV[1]
local rate = ARGV[2]
local period = ARGV[3]
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local increment = emission_interval * cost
local burst_offset = emission_interval * burst

-- redis returns time as an array containing two integers: seconds of the epoch
-- time (10 digits) and microseconds (6 digits). for convenience we need to
-- convert them to a floating point number. the resulting number is 16 digits,
-- bordering on the limits of a 64-bit double-precision floating point number.
-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
-- point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local tat = redis.call("GET", rate_limit_key)

if not tat then
  tat = now
else
  tat = tonumber(tat)
end

tat = math.max(tat, now)

local new_tat = tat + increment
local allow_at = new_tat - burst_offset

local diff = now - allow_at
local remaining = diff / emission_interval

if remaining < 0 then
  local reset_after = tat - now
  local retry_after = diff * -1
  return {
    0, -- allowed
    0, -- remaining
    tostring(retry_after),
    tostring(reset_after),
  }
end

local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end
local retry_after = -1
return {cost, remaining, tostring(retry_after), tostring(reset_after)}
`)

var allowAtMost = redis.NewScript(`
-- this script has side-effects, so it requires replicate commands mode
redis.replicate_commands()

local rate_limit_key = KEYS[1]
local burst = ARGV[1]
local rate = ARGV[2]
local period = ARGV[3]
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local burst_offset = emission_interval * burst

-- redis returns time as an array containing two integers: seconds of the epoch
-- time (10 digits) and microseconds (6 digits). for convenience we need to
-- convert them to a floating point number. the resulting number is 16 digits,
-- bordering on the limits of a 64-bit double-precision floating point number.
-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
-- point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local tat = redis.call("GET", rate_limit_key)

if not tat then
  tat = now
else
  tat = tonumber(tat)
end

tat = math.max(tat, now)

local diff = now - (tat - burst_offset)
local remaining = diff / emission_interval

if remaining < 1 then
  local reset_after = tat - now
  local retry_after = emission_interval - diff
  return {
    0, -- allowed
    0, -- remaining
    tostring(retry_after),
    tostring(reset_after),
  }
end

if remaining < cost then
  cost = remaining
  remaining = 0
else
  remaining = remaining - cost
end

local increment = emission_interval * cost
local new_tat = tat + increment

local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end

return {
  cost,
  remaining,
  tostring(-1),
  tostring(reset_after),
}
`)

const script = `
local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local fill_time = capacity/rate
local ttl = math.floor(fill_time*2)

local last_tokens = tonumber(redis.call("get", tokens_key))
if last_tokens == nil then
    last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
    last_refreshed = 0
end

local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
    new_tokens = filled_tokens - requested
end

redis.call("setex", tokens_key, ttl, new_tokens)
redis.call("setex", timestamp_key, ttl, now)

return { allowed, new_tokens }
`

// Client indicates the redis client of the rate limiter.
func Client() *redis.Client {
	return redisClient
}

// SetRedis sets the redis client.
func SetRedisWithConfig(config *ConfigRedis) error {
	if config == nil {
		return errors.New("redis config is empty")
	}

	redisClient = newRedisClient(*config)
	if redisClient == nil {
		return errors.New("redis client is nil")
	}

	go func() {
		timer := time.NewTicker(5 * time.Second)
		for range timer.C {
			loadScript()
		}
	}()
	return loadScript()
}

// SetRedis sets the redis client.
func SetRedis(redisCli *redis.Client) error {
	if redisCli == nil {
		return errors.New("redis client is nil")
	}
	redisClient = redisCli
	go func() {
		timer := time.NewTicker(5 * time.Second)
		for range timer.C {
			loadScript()
		}
	}()
	return loadScript()
}

func loadScript() error {
	if redisClient == nil {
		return errors.New("redis client is nil")
	}

	scriptHash = fmt.Sprintf("%x", sha1.Sum([]byte(script)))
	exists, err := redisClient.ScriptExists(context.Background(), scriptHash).Result()
	if err != nil {
		return err
	}

	// load script when missing.
	if !exists[0] {
		_, err := redisClient.ScriptLoad(context.Background(), script).Result()
		if err != nil {
			return err
		}
	}
	return nil
}

// Limit defines the maximum frequency of some events.
// Limit is represented as number of events per second.
// A zero Limit allows no events.
type Limit float64

// Inf is the infinite rate limit; it allows all events (even if burst is zero).
const Inf = Limit(math.MaxFloat64)

// A Limiter controls how frequently events are allowed to happen.
type Limiter struct {
	limit Limit
	burst int

	// mu sync.Mutex

	key string
}

// NewLimiter returns a new Limiter that allows events up to rate r and permits
// bursts of at most b tokens.
func NewLimiter(r Limit, b int, key string) *Limiter {
	return &Limiter{
		limit: r,
		burst: b,
		key:   key,
	}
}

// Every converts a minimum time interval between events to a Limit.
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

// Allow is shorthand for AllowN(time.Now(), 1).
func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

// AllowN reports whether n events may happen at time now.
// Use this method if you intend to drop / skip events that exceed the rate limit.
// Otherwise use Reserve or Wait.
func (lim *Limiter) AllowN(now time.Time, n int) bool {
	return lim.reserveN(now, n).ok
}

// A Reservation holds information about events that are permitted by a Limiter to happen after a delay.
// A Reservation may be canceled, which may enable the Limiter to permit additional events.
type Reservation struct {
	ok     bool
	tokens int
}

func (lim *Limiter) reserveN(now time.Time, n int) Reservation {
	if redisClient == nil {
		return Reservation{
			ok:     true,
			tokens: n,
		}
	}

	results, err := redisClient.EvalSha(
		context.Background(),
		scriptHash,
		[]string{lim.key + ".tokens", lim.key + ".ts"},
		float64(lim.limit),
		lim.burst,
		now.Unix(),
		n,
	).Result()
	if err != nil {
		log.Println("fail to call rate limit: ", err)
		return Reservation{
			ok:     true,
			tokens: n,
		}
	}

	rs, ok := results.([]interface{})
	if ok {
		newTokens, _ := rs[1].(int64)
		return Reservation{
			ok:     rs[0] == int64(1),
			tokens: int(newTokens),
		}
	}

	log.Println("fail to transform results")
	return Reservation{
		ok:     true,
		tokens: n,
	}
}
