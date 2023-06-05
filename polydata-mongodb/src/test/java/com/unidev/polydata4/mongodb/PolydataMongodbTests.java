package com.unidev.polydata4.mongodb;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.BasicPolyQuery;
import com.unidev.polydata4.domain.InsertRequest;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class PolydataMongodbTests {
    @Container
    private final GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);

    @Container
    private final GenericContainer redis = new GenericContainer("redis:7.0.5")
            .withExposedPorts(6379);


    String port;
    PolydataMongodb polydata;
    String polyId = "";


    @BeforeEach
    public void setUp() {
        port = mongodb.getMappedPort(27017) + "";

        polydata = new PolydataMongodb("mongodb://localhost:" + port + "/polydata4");
        polydata.prepareStorage();
        polyId = "poly_" + System.currentTimeMillis();
        BasicPoly poly = polydata.create(polyId);
        assertNotNull(poly);
    }

    @AfterEach
    public void cleanup() throws IOException {
        polydata.close();
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

    @Test
    void insert() {
        polydata.insert(polyId, Collections.singleton(InsertRequest.builder()
                .data(BasicPoly.newPoly("test").with("app", "123"))
                .indexToPersist(Set.of("tag1", "date"))
                .build()));

        BasicPolyList list = polydata.read(polyId, Set.of("test"));
        assertNotNull(list);
        assertThat(list.list().size()).isEqualTo(1);
        assertThat(list.hasPoly("test")).isTrue();
    }

    @Test
    void indexUpdate() {
        assertThat(polydata.index(polyId).isPresent()).isFalse();
        for (int i = 0; i < 100; i++) {
            polydata.insert(polyId, Collections.singletonList(
                    InsertRequest.builder()
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i, "tag_a_" + (i % 2), "_date"))
                            .build())
            );
        }

        BasicPoly index = polydata.index(polyId).get();
        assertThat(index).isNotNull();
        assertThat(index.data().isEmpty()).isFalse();

        assertThat(index.fetch("_date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(100);
        assertThat(index.fetch("tag_1", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
        assertThat(index.fetch("tag_a_0", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(50);
    }

    @Test
    void removal() {
        polydata.insert(polyId, Arrays.asList(
                        InsertRequest.builder()
                                .data(BasicPoly.newPoly("test").with("app", "123"))
                                .indexToPersist(Set.of("tag1", "date"))
                                .build(),
                        InsertRequest.builder()
                                .data(BasicPoly.newPoly("test2").with("app", "567"))
                                .indexToPersist(Set.of("tag2", "date"))
                                .build()
                )
        );

        assertThat(polydata.read(polyId, Set.of("test")).hasPoly("test")).isTrue();
        assertThat(polydata.index(polyId).get().fetch("tag1", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
        assertThat(polydata.index(polyId).get().fetch("date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(2);

        polydata.remove(polyId, Set.of("test"));
        assertThat(polydata.read(polyId, Set.of("test")).hasPoly("test")).isFalse();
        assertThat(polydata.index(polyId).get().containsKey("tag1")).isFalse();
        assertThat(polydata.index(polyId).get().fetch("date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
    }

    @Test
    void query() {
        for (int i = 0; i < 100; i++) {
            polydata.insert(polyId, Collections.singletonList(
                    InsertRequest.builder()
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i, "_date"))
                            .build())
            );
        }

        BasicPolyList list = polydata.query(polyId,
                BasicPolyQuery.builder().build());
        assertThat(list.list().size()).isEqualTo(10);
        assertThat(polydata.count(polyId, BasicPolyQuery.builder().build())).isEqualTo(100);

        BasicPolyQuery tagx = BasicPolyQuery.builder().build();
        tagx.index("tag_x");
        assertThat(polydata.count(polyId, tagx)).isEqualTo(100);

        BasicPolyQuery tag1 = BasicPolyQuery.builder().build();
        tag1.index("tag_1");
        assertThat(polydata.count(polyId, tag1)).isEqualTo(1);

    }

    @Test
    void queryPaging() {
        for (int i = 0; i < 105; i++) {
            polydata.insert(polyId, Collections.singletonList(
                    InsertRequest.builder()
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i, "_date"))
                            .build())
            );
        }
        int checkPoly = 104;
        for (int page = 0; page < 10; page++) {
            BasicPolyQuery query = new BasicPolyQuery();
            query.page(page);
            BasicPolyList list = polydata.query(polyId, query);
            assertThat(list.list().size()).isEqualTo(10);

            for (int i = 0; i < list.list().size(); i++) {
                assertTrue(list.hasPoly("test_" + checkPoly), "Missing poly " + "poly_" + checkPoly + " page: " + page);
                checkPoly--;
            }
        }
        BasicPolyQuery query = new BasicPolyQuery();
        query.page(10);
        BasicPolyList list = polydata.query(polyId, query);
        assertThat(list.list().size()).isEqualTo(5);

        for (int i = 0; i < list.list().size(); i++) {
            assertTrue(list.hasPoly("test_" + checkPoly), "Missing poly " + "poly_" + checkPoly + " page: 10");
            checkPoly--;
        }
    }

    @Test
    void multiplePolysHandling() {

        String poly1 = "poly-1-" + UUID.randomUUID();
        String poly2 = "poly-2-" + UUID.randomUUID();

        polydata.create(poly1);
        polydata.create(poly2);

        polydata.config(poly1, BasicPoly.newPoly(poly1).with("key-1", "value-1"));
        polydata.config(poly2, BasicPoly.newPoly(poly2).with("key-2", "value-2"));

        assertEquals(polydata.config(poly1).get().fetch("key-1", String.class), "value-1");
        assertEquals(polydata.config(poly1).get()._id(), poly1);

        assertEquals(polydata.config(poly2).get().fetch("key-2", String.class), "value-2");
        assertEquals(polydata.config(poly2).get()._id(), poly2);

        BasicPolyList polyList = polydata.list();
        assertEquals(3, polyList.list().size());
        assertTrue(polyList.hasPoly(poly1));
        assertTrue(polyList.hasPoly(poly2));
        assertTrue(polyList.hasPoly(polyId));

    }

    @Test
    void redisJcache() throws URISyntaxException, IOException {
        MutableConfiguration<String, BasicPoly> config = new MutableConfiguration<>();
        config.setStoreByValue(false);
        config.setStatisticsEnabled(true);
        File file = File.createTempFile("redisson-jcache", ".yaml");
        String port = redis.getMappedPort(6379) + "\"";
        FileUtils.write(file, """
                singleServerConfig:
                  address: "redis://127.0.0.1:""" + port + """ 
                """, StandardCharsets.UTF_8);

        CacheManager manager = Caching.getCachingProvider().getCacheManager(file.toURI(), null);
        Cache<String, BasicPoly> cache = manager.createCache("namedCache", config);

        cache.put("test", BasicPoly.newPoly("123"));
        cache.put("test2", BasicPoly.newPoly("987"));

        polydata.setCache(cache);


        String poly1 = "poly-1-" + UUID.randomUUID();
        String poly2 = "poly-2-" + UUID.randomUUID();

        polydata.create(poly1);
        polydata.create(poly2);

        polydata.config(poly1, BasicPoly.newPoly(poly1).with("key-1", "value-1"));
        polydata.config(poly2, BasicPoly.newPoly(poly2).with("key-2", "value-2"));

    }


}
