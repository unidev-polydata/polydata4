package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.BasicPolyQuery;
import com.unidev.polydata4.domain.InsertRequest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Generic integration test suite for polydata operations.
 */
public abstract class IntegrationTest {

    protected PolydataFactory factory = new PolydataFactory();

    protected Polydata polydata;

    protected void create(BasicPoly config) {
        polydata = factory.create(config).get();
    }

    @Test
    void polyCreation() {
        String poly = "random-name-" + System.currentTimeMillis();
        BasicPolyList list = polydata.list();
        assertNotNull(list);
        assertFalse(list.hasPoly(poly));
        polydata.create(poly);
        list = polydata.list();
        assertTrue(list.hasPoly(poly));
    }

    @Test
    void operationsById() {
        String poly = createPoly();
        BasicPoly data = BasicPoly.newPoly("test-id");
        data.put("test-key", "test-value");
        polydata.insert(poly, Set.of(InsertRequest.builder().poly(data).build()));
        BasicPolyList list = polydata.read(poly, Set.of("test-id"));
        assertNotNull(list);
        assertEquals(1, list.list().size());
        assertTrue(list.hasPoly("test-id"));
        BasicPoly dbPoly = list.safeById("test-id");
        assertEquals("test-id", dbPoly._id());
        assertEquals("test-value", dbPoly.fetch("test-key"));

        polydata.remove(poly, Set.of("test-id"));

        list = polydata.read(poly, Set.of("test-id"));
        assertNotNull(list);
        assertEquals(0, list.list().size());
    }


    @Test
    void query() {
        String poly = createPoly();

        for (int i = 0; i < 100; i++) {
            polydata.insert(poly, Collections.singletonList(
                    InsertRequest.builder()
                            .poly(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i))
                            .build())
            );
        }

        BasicPolyList list = polydata.query(poly,
                BasicPolyQuery.builder().build());
        assertThat(list.list().size()).isEqualTo(10);
        assertThat(polydata.count(poly, BasicPolyQuery.builder().build())).isEqualTo(100);

        BasicPolyQuery tagx = BasicPolyQuery.builder().build();
        tagx.index("tag_x");
        assertThat(polydata.count(poly, tagx)).isEqualTo(100);

        BasicPolyQuery tag1 = BasicPolyQuery.builder().build();
        tag1.index("tag_1");
        assertThat(polydata.count(poly, tag1)).isEqualTo(1);
    }

    @Test
    void queryPaging() {
        String poly = createPoly();
        for (int i = 0; i < 105; i++) {
            polydata.insert(poly, Collections.singletonList(
                    InsertRequest.builder()
                            .poly(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i))
                            .build())
            );
        }
        int checkPoly = 104;
        for (int page = 0; page < 10; page++) {
            BasicPolyQuery query = new BasicPolyQuery();
            query.page(page);
            BasicPolyList list = polydata.query(poly, query);
            assertThat(list.list().size()).isEqualTo(10);

            for (int i = 0; i < list.list().size(); i++) {
                assertTrue(list.hasPoly("test_" + checkPoly), "Missing poly " + "poly_" + checkPoly + " page: " + page);
                checkPoly--;
            }
        }
        BasicPolyQuery query = new BasicPolyQuery();
        query.page(10);
        BasicPolyList list = polydata.query(poly, query);
        assertThat(list.list().size()).isEqualTo(5);

        for (int i = 0; i < list.list().size(); i++) {
            assertTrue(list.hasPoly("test_" + checkPoly), "Missing poly " + "poly_" + checkPoly + " page: 10");
            checkPoly--;
        }
    }

    String createPoly() {
        String poly = "poly-" + System.currentTimeMillis();
        polydata.create(poly);
        return poly;
    }


}
