package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Optional;

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
        assertEquals(3, tag1.fetch("_count", 0));

        BasicPoly date = testIndex.fetch("_date");
        assertEquals(3, date.fetch("_count", 0));

        BasicPoly tag2 = polydataYaml.indexData("test1", "tag2").get();
        assertEquals(1, tag2.fetch("_count", 0));
    }

}
