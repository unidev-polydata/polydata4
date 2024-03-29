package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.flatfiles.PolydataSingleJson;
import com.unidev.polydata4.flatfiles.PolydataYaml;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PolydataFactoryTest {

    PolydataFactory polydataFactory = new PolydataFactory();

    @Test
    void mongodb() {
        BasicPoly config = new BasicPoly();
        config.put("type", "mongodb");
        config.put("uri", "mongodb://localhost:27017");

        Optional<Polydata> polydata = polydataFactory.create(config);
        assertTrue(polydata.isPresent());
    }

    @Test
    void mongodbCache() {
        BasicPoly config = new BasicPoly();
        config.put("type", "mongodb");
        config.put("uri", "mongodb://localhost:27017");

        config.put("cache", BasicPoly.newPoly()
                .with("type", "jcache")
                .with("provider", "org.ehcache.jsr107.EhcacheCachingProvider")
                .with("name", "polydata")
        );

        Optional<Polydata> polydata = polydataFactory.create(config);
        assertTrue(polydata.isPresent());
    }

    @Test
    void notExistingFactory() {
        BasicPoly config = new BasicPoly();
        config.put("type", "test-qwe");

        Optional<Polydata> polydata = polydataFactory.create(config);
        assertFalse(polydata.isPresent());
    }

    @Test
    void flatFileFactory() {
        BasicPoly config = new BasicPoly();
        config.put("type", "flat-file-yaml");
        config.put("root", "../polydata-flat-files/polydata-yaml");

        Optional<Polydata> polydata = polydataFactory.create(config);
        assertTrue(polydata.isPresent());
        assertTrue(polydata.get() instanceof PolydataYaml);
    }

    @Test
    void flatFileSingleJson() {
        new File("/tmp/flat-file-json").mkdirs();

        BasicPoly config = new BasicPoly();
        config.put("type", "flat-file-json");
        config.put("root", "/tmp/flat-file-json");

        Optional<Polydata> polydata = polydataFactory.create(config);
        assertTrue(polydata.isPresent());
        assertTrue(polydata.get() instanceof PolydataSingleJson);
    }

}
