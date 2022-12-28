package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.BasicPolyQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PolydataYamlTest {

    private final PolydataYaml polydataYaml = new PolydataYaml(new File("polydata-yaml"));

    @BeforeEach
    public void init() {
        polydataYaml.prepareStorage();
    }

    @Test
    void loading() {
        Map<String, FlatFileRepository> repositories = polydataYaml.getRepositories();

        assertNotNull(repositories);
        assertEquals(2, repositories.size());

        assertTrue(repositories.containsKey("test1"));
        assertTrue(repositories.containsKey("test2"));

        assertEquals(2, polydataYaml.list().list().size());
    }

    @Test
    void index() {
        assertTrue(polydataYaml.index("qwe").isEmpty());
        BasicPoly testIndex = polydataYaml.index("test1").get();
        assertEquals(4, testIndex.getData().size());
        assertTrue(testIndex.containsKey("tag1"));
        assertTrue(testIndex.containsKey("tag2"));
        assertTrue(testIndex.containsKey("tag3"));
        assertTrue(testIndex.containsKey("_date"));

        BasicPoly tag1 = testIndex.fetch("tag1");
        assertEquals(4, tag1.fetch("_count", 0));

        BasicPoly date = testIndex.fetch("_date");
        assertEquals(4, date.fetch("_count", 0));

        BasicPoly tag2 = polydataYaml.indexData("test1", "tag2").get();
        assertEquals(1, tag2.fetch("_count", 0));
    }

    @Test
    void query() {
        BasicPolyList list = polydataYaml.query("test1", BasicPolyQuery.builder().build());
        assertEquals(2, list.list().size());

        assertTrue(list.hasPoly("test-id-4"));
        assertTrue(list.hasPoly("test-id-3"));

        BasicPolyQuery query2 = new BasicPolyQuery();
        query2.page(1);
        BasicPolyList listPage2 = polydataYaml.query("test1", query2);
        assertEquals(2, listPage2.list().size());

        assertTrue(listPage2.hasPoly("test-id-2"));
        assertTrue(listPage2.hasPoly("test-id-1"));

        BasicPolyQuery query3 = new BasicPolyQuery();
        query3.page(2);
        BasicPolyList listPage3 = polydataYaml.query("test1", query3);
        assertEquals(0, listPage3.list().size());
    }

    @Test
    void count() {
        Long countTest1 = polydataYaml.count("test1", BasicPolyQuery.builder().build());
        assertEquals(4, countTest1);
    }

    @Test
    void queryIndex() {
        BasicPolyQuery query = new BasicPolyQuery();
        query.index("tag2");
        BasicPolyList listPage = polydataYaml.query("test1", query);
        assertEquals(1, listPage.list().size());
    }

    @Test
    void countIndex() {
        BasicPolyQuery query = new BasicPolyQuery();
        query.index("tag2");
        Long count = polydataYaml.count("test1", query);
        assertEquals(1, count);
    }

}

