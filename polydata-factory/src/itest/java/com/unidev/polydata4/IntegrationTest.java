package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.InsertRequest;
import org.junit.jupiter.api.Test;

import java.util.Set;

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
    }

    String createPoly() {
        String poly = "poly-" + System.currentTimeMillis();
        polydata.create(poly);
        return poly;
    }


}
