package com.unidev.polydata4.flatfiles.yaml;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PolydataYamlTest {

    @Test
    public void yamlConfigReading() throws IOException {
        YamlFile yamlFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/config.yaml"), YamlFile.class);
        assertEquals("cvalue1", yamlFile.get("cfield1"));
        assertNotNull(yamlFile.getMetadata());
        assertEquals("c1-value", yamlFile.getMetadata().get("c1"));
    }

    @Test
    public void yamlPolyReading() throws IOException {
        YamlFile yamlFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/data/a/a.yaml"), YamlFile.class);
        assertEquals("123", yamlFile.get("666"));
        assertNotNull(yamlFile.getMetadata());
        assertEquals("ccc", yamlFile.getMetadata().get("bbb"));

        List<String> indexes = yamlFile.getMetadata().getIndex();
        assertNotNull(indexes);
        assertEquals(3, indexes.size());
        assertTrue(indexes.contains("tag1"));
        assertTrue(indexes.contains("tag2"));
        assertTrue(indexes.contains("tag3"));

    }

    @Test
    public void yamlToPoly() throws IOException {
        YamlFile yamlFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/data/a/a.yaml"), YamlFile.class);
        BasicPoly basicPoly = yamlFile.toPoly();
        assertNotNull(basicPoly);
        assertEquals("test-id-2", basicPoly.fetch("_id"));
        assertEquals(123, (Integer) basicPoly.fetch("xxx"));
        Map<String, Object> metadata = basicPoly.metadata();
        assertNotNull(metadata);
        assertEquals("ccc", metadata.get("bbb"));

        List<String> indexes = (List<String>) metadata.get("_index");
        assertNotNull(indexes);
        assertEquals(3, indexes.size());
        assertTrue(indexes.contains("tag1"));
        assertTrue(indexes.contains("tag2"));
        assertTrue(indexes.contains("tag3"));
    }
}
