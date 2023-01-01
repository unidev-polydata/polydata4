package com.unidev.polydata4.sqlite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class PolydataSqliteTest {

    @TempDir
    File root;

    PolydataSqlite polydata;

    @BeforeEach
    void init() {
        root.mkdirs();
        polydata = new PolydataSqlite(root);
    }


    @Test
    void polyCreation() {
        polydata.create("test");
    }

}
