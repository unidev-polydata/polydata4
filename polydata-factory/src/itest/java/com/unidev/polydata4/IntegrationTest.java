package com.unidev.polydata4;

import com.unidev.platform.template.TemplateBuilder;
import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.BasicPolyQuery;
import com.unidev.polydata4.domain.InsertRequest;
import freemarker.template.Template;
import org.junit.jupiter.api.Test;

import java.util.*;

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
    void listPolys() {
        polydata.create("poly1");
        polydata.create("poly2");
        polydata.create("poly3");
        polydata.create("poly4");
        polydata.create("poly5");

        BasicPolyList list = polydata.list();
        assertEquals(5, list.list().size());
    }

    @Test
    void operationsById() {
        String poly = createPoly();
        BasicPoly data = BasicPoly.newPoly("test-id");
        data.put("test-key", "test-value");
        polydata.insert(poly, Set.of(InsertRequest.builder().data(data).build()));
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
    void operationsByMultipleIds() {
        String poly = createPoly();
        Set<InsertRequest> items = new HashSet<>();
        for (int i = 1; i <= 100; i++) {
            BasicPoly data = BasicPoly.newPoly("test-id-" + i);
            data.put("item", "item " + i);
            data.put("id", i);
            data.put("date", new Date());
            items.add(InsertRequest.builder().data(data).build());
        }
        polydata.insert(poly, items);

        Set<String> queryIds = new HashSet<>();
        for (int i = 1; i <= 100; i++) {
            queryIds.add("test-id-" + i);
        }
        BasicPolyList list = polydata.read(poly, queryIds);
        assertNotNull(list);
        assertEquals(100, list.list().size());
    }

    @Test
    void indexListing() {
        String poly = createPoly();
        List<InsertRequest> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(InsertRequest.builder()
                    .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                    .indexToPersist(Set.of("id_" + (i % 2), "tag_" + i))
                    .build());

        }
        polydata.insert(poly, list);
        BasicPoly index = polydata.index(poly).get();
        assertNotNull(index);
        assertEquals("50", index.fetch("id_0", BasicPoly.class).fetch("count") + "");
        assertEquals("50", index.fetch("id_1", BasicPoly.class).fetch("count") + "");
        for (int i = 1; i <= 100; i++) {
            assertEquals("1", index.fetch("tag_" + i, BasicPoly.class).fetch("count") + "");
        }
    }

    @Test
    void updateIndexes() {
        String poly = createPoly();
        // insert test data
        List<InsertRequest> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(InsertRequest.builder()
                    .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                    .indexToPersist(Set.of("id_" + (i % 2), "tag_" + i))
                    .build());

        }
        polydata.insert(poly, list);

        list.clear();
        for (int i = 1; i <= 100; i++) {
            list.add(InsertRequest.builder()
                    .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                    .indexToPersist(Set.of("id_" + (i % 4), "tag_" + i))
                    .build());

        }
        polydata.insert(poly, list);

        BasicPoly index = polydata.index(poly).get();
        assertNotNull(index);

        assertEquals("25", index.fetch("id_0", BasicPoly.class).fetch("count") + "");
        assertEquals("25", index.fetch("id_1", BasicPoly.class).fetch("count") + "");
        assertEquals("25", index.fetch("id_2", BasicPoly.class).fetch("count") + "");
        assertEquals("25", index.fetch("id_3", BasicPoly.class).fetch("count") + "");
        for (int i = 1; i <= 100; i++) {
            assertEquals("1", index.fetch("tag_" + i, BasicPoly.class).fetch("count") + "");
        }
    }

    @Test
    void updateIndexOnRemoval() {
        String poly = createPoly();
        // insert test data
        List<InsertRequest> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(InsertRequest.builder()
                    .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                    .indexToPersist(Set.of("id_" + (i % 2), "tag_" + i))
                    .build());

        }
        polydata.insert(poly, list);

        // remove half of the data

        Set<String> removeIds = new HashSet<>();
        for (int i = 1; i <= 100; i++) {
            if (i % 2 == 0) {
                removeIds.add("test_" + i);
            }
        }
        polydata.remove(poly, removeIds);
        BasicPoly index = polydata.index(poly).get();
        assertNotNull(index);
        assertEquals("50", index.fetch("_date", BasicPoly.class).fetch("count") + "");
        assertEquals("50", index.fetch("id_1", BasicPoly.class).fetch("count") + "");
    }

    @Test
    void query() {
        String poly = createPoly();

        for (int i = 0; i < 100; i++) {
            polydata.insert(poly, Collections.singletonList(
                    InsertRequest.builder()
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
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
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
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

    @Test
    void configOperations() {
        String poly = createPoly();
        BasicPoly config = polydata.config(poly).get();
        config.put("test-key", "test-value");
        polydata.config(poly, config);

        BasicPoly config2 = polydata.config(poly).get();
        assertEquals("test-value", config2.fetch("test-key"));
    }

    @Test
    void metadataOperations() {
        String poly = createPoly();
        BasicPoly metadata = polydata.metadata(poly).get();
        metadata.put("test-key", "test-value");
        polydata.metadata(poly, metadata);

        BasicPoly metadata2 = polydata.metadata(poly).get();
        assertEquals("test-value", metadata2.fetch("test-key"));
    }

    @Test
    void queryRandom() {
        String poly = createPoly();
        for (int i = 0; i < 105; i++) {
            polydata.insert(poly, Collections.singletonList(
                    InsertRequest.builder()
                            .data(BasicPoly.newPoly("test_" + i).with("app", i + "").with("field", i))
                            .indexToPersist(Set.of("tag_x", "tag_" + i))
                            .build())
            );
        }
        BasicPolyQuery query = new BasicPolyQuery();
        query.queryType(BasicPolyQuery.QueryFunction.RANDOM);
        query.withOption("random_count", 50);

        BasicPolyList list = polydata.query(poly, query);
        assertThat(list.list().size()).isEqualTo(50);
    }

    String createPoly() {
        String poly = "poly-" + System.currentTimeMillis();
        polydata.create(poly);
        return poly;
    }

    public static String renderTemplate(String content, Map<String, Object> variables) {
        try {
            Template template = TemplateBuilder.newTemplate(content).build().get();
            String renderedFile = TemplateBuilder.evaluate(template, variables).get();
            return renderedFile;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
