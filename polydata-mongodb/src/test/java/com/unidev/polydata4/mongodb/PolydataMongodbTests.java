package com.unidev.polydata4.mongodb;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.BasicPolyQuery;
import com.unidev.polydata4.domain.PersistRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class PolydataMongodbTests {
    @Container
    private GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);
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
        polydata.insert(polyId, Collections.singleton(PersistRequest.builder()
                .poly(BasicPoly.newPoly("test").with("app", "123"))
                .indexToPersist(Set.of("tag1", "date"))
                .build()));

        BasicPolyList list = polydata.read(polyId, Set.of("test"));
        assertNotNull(list);
        assertThat(list.list().size()).isEqualTo(1);
        assertThat(list.hasPoly("test")).isTrue();
    }

    @Test
    void indexUpdate() {
        BasicPoly index = polydata.index(polyId);
        assertThat(index).isNotNull();
        assertThat(index.data().isEmpty()).isTrue();

        for (int i = 0; i < 100; i++) {
            polydata.insert(polyId, Arrays.asList(
                    PersistRequest.builder()
                            .poly(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i, "tag_a_" + (i % 2)))
                            .build())
            );
        }

        index = polydata.index(polyId);
        assertThat(index).isNotNull();
        assertThat(index.data().isEmpty()).isFalse();

        assertThat(index.fetch("_date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(100);
        assertThat(index.fetch("tag_1", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
        assertThat(index.fetch("tag_a_0", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(50);
    }

    @Test
    void removal() {
        polydata.insert(polyId, Arrays.asList(
                        PersistRequest.builder()
                                .poly(BasicPoly.newPoly("test").with("app", "123"))
                                .indexToPersist(Set.of("tag1", "date"))
                                .build(),
                        PersistRequest.builder()
                                .poly(BasicPoly.newPoly("test2").with("app", "567"))
                                .indexToPersist(Set.of("tag2", "date"))
                                .build()
                )
        );

        assertThat(polydata.read(polyId, Set.of("test")).hasPoly("test")).isTrue();
        assertThat(polydata.index(polyId).fetch("tag1", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
        assertThat(polydata.index(polyId).fetch("date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(2);

        polydata.remove(polyId, Set.of("test"));
        assertThat(polydata.read(polyId, Set.of("test")).hasPoly("test")).isFalse();
        assertThat(polydata.index(polyId).fetch("tag1", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(0);
        assertThat(polydata.index(polyId).fetch("date", BasicPoly.class).fetch("count", Integer.class)).isEqualTo(1);
    }

    @Test
    void query() {
        for (int i = 0; i < 100; i++) {
            polydata.insert(polyId, Arrays.asList(
                    PersistRequest.builder()
                            .poly(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i))
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
            polydata.insert(polyId, Arrays.asList(
                    PersistRequest.builder()
                            .poly(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i))
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


}
