package com.unidev.polydata4;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

public class FlatFileJsonIntegrationTest extends IntegrationTest {

    @TempDir
    File tempDir;

    @BeforeEach
    public void setup() {
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "flat-file-json")
                .with("root", tempDir.getAbsolutePath());
        create(config);
    }

}
