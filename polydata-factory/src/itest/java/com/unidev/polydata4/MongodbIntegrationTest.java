package com.unidev.polydata4;

import com.unidev.polydata4.domain.*;
import com.unidev.polydata4.mongodb.PolydataMongodb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.Set;

import static com.unidev.polydata4.api.Polydata.CUSTOM_QUERY;
import static com.unidev.polydata4.api.Polydata.SEARCH_TEXT;
import static com.unidev.polydata4.domain.BasicPolyQuery.QUERY_FUNCTION;
import static org.junit.jupiter.api.Assertions.*;

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
    void mongoDbCustomQuery() {
        String poly = createPoly();
        for (int i = 0; i < 1000; i++) {
            polydata.insert(poly,
                    InsertOptions.builder().build(),
                    Collections.singletonList(
                            InsertRequest.builder()
                                    .data(BasicPoly.newPoly("test_" + i).with("app", "app_" + i).with("field", i))
                                    .indexToPersist(Set.of("tag_x", "tag_" + i, "_date"))
                                    .build())
            );
        }

        PolydataMongodb polydataMongodb = (PolydataMongodb) polydata;
        polydataMongodb.createTextIndex(poly, "_id", "_indexes", "app");
        BasicPolyList list = polydata.query(poly,
                BasicPolyQuery
                        .builder()
                        .options(
                                BasicPoly.newPoly()
                                        .with(QUERY_FUNCTION, BasicPolyQuery.QueryFunction.CUSTOM.name())
                                        .with(CUSTOM_QUERY, """                       
                        {
                           app: { $regex: /app_.*/ }
                        }"""
                        )).build());
        assertEquals(10, list.list().size());
        for (int i = 999; i > 989; i--) {
            assertTrue(list.hasPoly("test_" + i), "Missing poly " + i);
        }

        list = polydata.query(poly,
                BasicPolyQuery
                        .builder()
                        .options(
                                BasicPoly.newPoly()
                                        .with(QUERY_FUNCTION, BasicPolyQuery.QueryFunction.CUSTOM.name())
                                        .with(CUSTOM_QUERY, """                       
                        {
                          $and: [
                            { app: "app_10" },
                            { field: { $eq: 10 } }
                          ]
                        }"""
                                        )).build());
        assertEquals(1, list.list().size());
        assertTrue(list.hasPoly("test_10"));

    }

    @Test
    void mongoDbSearch() {
        String poly = createPoly();
        for (int i = 0; i < 1000; i++) {
            polydata.insert(poly,
                    InsertOptions.builder().build(),
                    Collections.singletonList(
                            InsertRequest.builder()
                                    .data(BasicPoly.newPoly("test_" + i).with("app", "app_" + i).with("field", i))
                                    .indexToPersist(Set.of("tag_x", "tag_" + i, "_date"))
                                    .build())
            );
        }

        PolydataMongodb polydataMongodb = (PolydataMongodb) polydata;
        polydataMongodb.createTextIndex(poly, "_id", "_indexes", "app");

        BasicPolyList list = polydata.query(poly,
                BasicPolyQuery
                        .builder()
                        .options(
                                BasicPoly.newPoly()
                                        .with(QUERY_FUNCTION, BasicPolyQuery.QueryFunction.SEARCH.name())
                                        .with(SEARCH_TEXT, "app_0")

                        ).build());
        assertFalse(list.list().isEmpty());
        assertEquals(1, list.list().size());
        assertTrue(list.hasPoly("test_0"));

        list = polydata.query(poly,
                BasicPolyQuery
                        .builder()
                        .options(
                                BasicPoly.newPoly()
                                        .with(QUERY_FUNCTION, BasicPolyQuery.QueryFunction.SEARCH.name())
                                        .with(SEARCH_TEXT, "tag_x")

                        ).build());
        assertEquals(10, list.list().size());
        for (int i = 999; i > 989; i--) {
            assertTrue(list.hasPoly("test_" + i), "Missing poly " + i);
        }
    }

}
