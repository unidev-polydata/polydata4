package com.unidev.polydata4.flatfiles;

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
    public void loading() {
        Map<String, FlatFileRepository> repositories = polydataYaml.getRepositories();

        assertNotNull(repositories);
        assertEquals(2, repositories.size());

        assertTrue(repositories.containsKey("test1"));
        assertTrue(repositories.containsKey("test2"));

        assertEquals(2, polydataYaml.list().list().size());
    }

}
