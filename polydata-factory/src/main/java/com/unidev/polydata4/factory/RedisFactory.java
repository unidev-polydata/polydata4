package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.api.packer.NoOpPolyPacker;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.redis.PolydataRedis;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Optional;

@Slf4j
public class RedisFactory implements StorageFactory {

    @Override
    public String type() {
        return "redis";
    }

    @Override
    public Optional<Polydata> create(BasicPoly config) {
        if (!config.containsKey("uri")) {
            log.warn("Missing URI");
            return Optional.empty();
        }
        String uri = config.fetch("uri");
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.fetch("max-total", 100));
        poolConfig.setMaxIdle(config.fetch("max-idle", 10));
        poolConfig.setMinIdle(config.fetch("min-idle", 10));
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(config.fetch("min-evictable-idle", 60)).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(config.fetch("time-between-eviction-runs", 30)).toMillis());
        poolConfig.setNumTestsPerEvictionRun(config.fetch("num-tests-per-eviction", 10));

        JedisPool jedisPool = new JedisPool(poolConfig, uri);

        PolydataRedis polydataRedis = new PolydataRedis(

                PolydataRedis.PolydataRedisConfig.builder()
                        .pool(jedisPool)
                        .hashIds(config.fetch("hash-ids", false))
                        .prefix(config.fetch("prefix", "prod"))
                        .polyPacker(new NoOpPolyPacker())
                        .build()

        );
        return Optional.of(polydataRedis);
    }

}
