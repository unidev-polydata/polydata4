package com.unidev.polydata4.redis;

import com.unidev.polydata4.api.packer.NoOpPolyPacker;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class PolydataRedisTest {

    @Container
    private final GenericContainer redis = new GenericContainer("redis:7.0.5")
            .withExposedPorts(6379);

    PolydataRedis polydata;
    String polyId = "";

    @BeforeEach
    public void setUp() {

        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);

        JedisPool jedisPool = new JedisPool(poolConfig, redis.getHost(), redis.getMappedPort(6379));

        polydata = new PolydataRedis(
                PolydataRedis.PolydataRedisConfig.builder()
                        .pool(jedisPool)
                        .polyPacker(new NoOpPolyPacker())
                        .build()
        );

        polydata.prepareStorage();
        polyId = "poly_" + System.currentTimeMillis();
        BasicPoly poly = polydata.create(polyId);
        assertNotNull(poly);
    }

    @Test
    public void listPolys() {
        BasicPolyList list = polydata.list();
        assertNotNull(list);
        assertEquals(1, list.list().size());
        assertTrue(list.hasPoly(polyId));
    }

}
