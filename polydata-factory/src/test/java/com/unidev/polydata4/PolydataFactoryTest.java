package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.Test;

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
                .with("name", "org.ehcache.jsr107.EhcacheCachingProvider")
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

}
