package com.unidev.polydata4;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MongodbIntegrationTest extends IntegrationTest {

    @Container
    private final GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);

    @Container
    private final GenericContainer redis = new GenericContainer("redis:7.0.5")
            .withExposedPorts(6379);


    @BeforeEach
    public void setup() {
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "mongodb")
                .with("uri", "mongodb://localhost:" + mongodb.getMappedPort(27017) + "/polydata4");
        create(config);
    }


}
