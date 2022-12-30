package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import org.junit.jupiter.api.Test;

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


}
