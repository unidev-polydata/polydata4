package com.unidev.polydata4.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PolydataSqliteTest {

    @TempDir
    File root;

    PolydataSqlite polydata;

    @BeforeEach
    void init() {
        root.mkdirs();
        polydata = new PolydataSqlite(root, new ObjectMapper());
    }


    @Test
    void polyCreation() {
        polydata.create("test");
    }

    @Test
    void genHash() {
        long v1 = polydata.genHash("123");
        long v2 = polydata.genHash("123");
        assertEquals(v1, v2);
    }

}
