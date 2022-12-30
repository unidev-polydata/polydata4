package com.unidev.polydata4;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RedisIntegrationTest extends IntegrationTest {

    @Container
    private final GenericContainer redis = new GenericContainer("redis:7.0.5")
            .withExposedPorts(6379);

    @BeforeEach
    public void setup() {
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "redis")
                .with("uri", "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379))
                ;
        create(config);
    }


}
