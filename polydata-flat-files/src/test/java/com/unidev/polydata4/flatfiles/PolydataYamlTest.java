package com.unidev.polydata4.flatfiles;

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
        FlatFile flatFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/polydata.yaml"), FlatFile.class);
        assertEquals("cvalue1", flatFile.get("cfield1"));
        assertNotNull(flatFile.getMetadata());
        assertEquals("metavalue2", flatFile.getMetadata().get("metakey2"));
    }

    @Test
    public void yamlPolyReading() throws IOException {
        FlatFile flatFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/data/a/a.yaml"), FlatFile.class);
        assertEquals("123", flatFile.get("666"));
        assertNotNull(flatFile.getMetadata());
        assertEquals("ccc", flatFile.getMetadata().get("bbb"));

        List<String> indexes = flatFile.getMetadata().getIndex();
        assertNotNull(indexes);
        assertEquals(3, indexes.size());
        assertTrue(indexes.contains("tag1"));
        assertTrue(indexes.contains("tag2"));
        assertTrue(indexes.contains("tag3"));

    }

    @Test
    public void yamlToPoly() throws IOException {
        FlatFile flatFile = PolydataYaml.MAPPER.readValue(new File("polydata-yaml/test1/data/a/a.yaml"), FlatFile.class);
        BasicPoly basicPoly = flatFile.toPoly();
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
