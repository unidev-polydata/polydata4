package com.unidev.polydata4.flatfiles;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class PolydataSingleJsonTest {

    @TempDir
    File tmpDir;

    @Test
    void polydataLoading() throws IOException {
        PolydataSingleJson polydata = new PolydataSingleJson(tmpDir);
        polydata.open();
        polydata.prepareStorage();
        polydata.create("test");
        assertTrue(polydata.exists("test"));
        polydata.close();


        PolydataSingleJson polydata2 = new PolydataSingleJson(tmpDir);
        polydata2.open();
        polydata2.prepareStorage();
        assertTrue(polydata.exists("test"));
    }

}
