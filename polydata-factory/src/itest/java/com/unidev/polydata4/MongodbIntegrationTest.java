package com.unidev.polydata4;

import com.unidev.polydata4.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static com.unidev.polydata4.api.Polydata.SEARCH_TEXT;
import static com.unidev.polydata4.domain.BasicPolyQuery.QUERY_FUNCTION;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class MongodbIntegrationTest extends IntegrationTest {

    @Container
    private final GenericContainer mongodb = new GenericContainer("mongo:6.0.2-focal")
            .withExposedPorts(27017);

    @BeforeEach
    public void setup() {
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "mongodb")
                .with("uri", "mongodb://localhost:" + mongodb.getMappedPort(27017) + "/polydata4");
        create(config);
    }

    @Test
    void mongoDbSearch() {
        String poly = createPoly();
        for (int i = 0; i < 1000; i++) {
            polydata.insert(poly,
                    InsertOptions.builder().build(),
                    Collections.singletonList(
                            InsertRequest.builder()
                                    .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                                    .indexToPersist(Set.of("tag_x", "tag_" + i, "_date"))
                                    .build())
            );
        }

        BasicPolyList list = polydata.query(poly,
                BasicPolyQuery
                        .builder()
                        .options(
                                BasicPoly.newPoly()
                                        .with(QUERY_FUNCTION, BasicPolyQuery.QueryFunction.SEARCH.name())
                                        .with(SEARCH_TEXT, "test_0")

                        ).build());
        assertFalse(list.list().isEmpty());
    }

}
