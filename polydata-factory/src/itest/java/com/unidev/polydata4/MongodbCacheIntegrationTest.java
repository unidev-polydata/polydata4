package com.unidev.polydata4;

import com.unidev.polydata4.domain.BasicPoly;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Mongodb integration tests with ehcache.
 */
@Testcontainers
public class MongodbCacheIntegrationTest extends IntegrationTest {

    @Container
    private final GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);

    @Container
    private final GenericContainer redis = new GenericContainer("redis:7.0.5")
            .withExposedPorts(6379);


    @BeforeEach
    public void setup() throws IOException {

        InputStream resourceAsStream = getClass().getResourceAsStream("/cache/ehcache.xml");
        String resource = IOUtils.toString(resourceAsStream, Charset.defaultCharset());
        int random = (int) (Math.random() * 10000);
        resource = renderTemplate(resource, Map.of("random", random));
        new File("/tmp/ehcache-" + random).mkdirs();
        // write stream to temp file
        File tempFile = File.createTempFile("ehcache", ".xml");
        FileUtils.writeStringToFile(tempFile, resource, Charset.defaultCharset());
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "mongodb")
                .with("uri", "mongodb://localhost:" + mongodb.getMappedPort(27017) + "/polydata4")
                .with("cache", BasicPoly.newPoly()
                        .with("type", "jcache")
                        .with("provider", "org.ehcache.jsr107.EhcacheCachingProvider")
                        .with("name", "polydata")
                        .with("implementationUri", "file://" + tempFile.getAbsolutePath())
                );

        create(config);
    }


}
