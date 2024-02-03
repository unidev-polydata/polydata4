package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generation of polydata index from flat JSON files.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataFlatIndex {

    public static final String INDEX_FILE = "polydata.json";
    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        FlatFileDeserializer.install(MAPPER);
    }

    @Getter
    private final File rootDir;

    @Getter
    private final Map<String, FlatFileRepository> repositories = new ConcurrentHashMap<>();

    public void scanIndex() {

    }


    public static class PolydataFlatFile {



    }

}
