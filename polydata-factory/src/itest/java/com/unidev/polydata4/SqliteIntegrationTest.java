package com.unidev.polydata4;

import com.unidev.polydata4.domain.BasicPoly;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;

@Testcontainers
public class SqliteIntegrationTest extends IntegrationTest {

    @TempDir
    File root;

    @BeforeEach
    public void setup() {
        root.mkdirs();
        BasicPoly config = BasicPoly.newPoly()
                .with("type", "sqlite")
                .with("root", root.getAbsolutePath())
                ;
        create(config);
    }


}
