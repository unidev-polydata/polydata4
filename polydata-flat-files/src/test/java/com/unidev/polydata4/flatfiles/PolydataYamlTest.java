package com.unidev.polydata4.flatfiles;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

public class PolydataYamlTest {

    private PolydataYaml polydataYaml = new PolydataYaml(new File("polydata-yaml"));

    @BeforeEach
    public void init() {
        polydataYaml.prepareStorage();
    }

    @Test
    public void loading() {

    }

}
