package com.unidev.polydata4.mongodb;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
public class PolydataMongodbTests {


    @Container
    private GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);
    String port;
    PolydataMongodb polydata;
    String polyId = "";


    @BeforeEach
    public void setUp() throws IOException, InterruptedException {
        port = mongodb.getMappedPort(27017) + "";

        polydata = new PolydataMongodb("mongodb://localhost:"+port+"/polydata4");
        polydata.prepareStorage();
        polyId = "poly-" + System.currentTimeMillis();
        BasicPoly poly = polydata.create(polyId);
        assertNotNull(poly);
    }

    @AfterEach
    public void after() throws IOException, InterruptedException {
        System.out.println(mongodb.execInContainer("mongosh", "mongodb://127.0.0.1:27017/polydata4", "--eval", "show collections"));

    }

    @Test
    void polyCreation() throws IOException, InterruptedException {

        String testPoly = "random-poly-" + System.currentTimeMillis();
        BasicPoly basicPoly = polydata.create(testPoly);
        assertThat(basicPoly).isNotNull();
        BasicPolyList list = polydata.list();
        assertThat(list.hasPoly(testPoly)).isTrue();
    }

    @Test
    void polyConfigAndMetadata() {

        String configData = "config-data-" + System.currentTimeMillis();
        String metadata = "metadata-" + System.currentTimeMillis();

        polydata.config(polyId, BasicPoly.newPoly(polyId).with("config", configData));
        polydata.metadata(polyId, BasicPoly.newPoly(polyId).with("metadata", metadata));

        Optional<BasicPoly> polyMeta = polydata.metadata(polyId);
        Optional<BasicPoly> polyConfig = polydata.config(polyId);

        assertThat(polyMeta).isPresent();
        assertThat(polyConfig).isPresent();

        assertThat(polyMeta.get().fetch("metadata") + "").isEqualTo(metadata);
        assertThat(polyConfig.get().fetch("config") + "").isEqualTo(configData);

    }

}
