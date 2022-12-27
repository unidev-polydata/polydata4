package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.PersistRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polydata storage backed by Yaml files
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataYaml extends AbstractPolydata {

    public static final String DATE_INDEX = "_date";
    public static final String DATA_DIR = "data";
    public static final String POLY_FILE = "polydata.yaml";
    private static final String[] POLY_EXTENSIONS = new String[]{"yaml", "yml"};
    public static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    static {
        SimpleModule flatFile =
                new SimpleModule("FlatFileDeserializer", new Version(1, 0, 0, null, null, null));
        flatFile.addDeserializer(FlatFile.class, new FlatFileDeserializer(FlatFile.class, MAPPER));
        MAPPER.registerModule(flatFile);

        SimpleModule fileMetadata =
                new SimpleModule("FlatFileDeserializer", new Version(1, 0, 0, null, null, null));
        flatFile.addDeserializer(FlatFile.FileMetadata.class, new FileMetadataDeserializer(FlatFile.FileMetadata.class, MAPPER));
        MAPPER.registerModule(fileMetadata);
    }

    @Getter
    private final File rootDir;

    @Getter
    private final Map<String, FlatFileRepository> repositories = new ConcurrentHashMap<>();

    /**
     * Scan root directory for polys
     */
    @Override
    public void prepareStorage() {

        File[] files = rootDir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                loadPoly(file);
            }
        }
    }

    /**
     * Load specific poly directory
     *
     * @param polyDir
     */
    public void loadPoly(File polyDir) {
        log.info("Loading poly {}", polyDir.getName());
        FlatFileRepository flatFileRepository = new FlatFileRepository();
        flatFileRepository.setPoly(polyDir.getName());

        // load poly file
        File polyFile = new File(polyDir, POLY_FILE);
        if (polyFile.exists()) {
            try {
                FlatFile flatFile = MAPPER.readValue(polyFile, FlatFile.class);
                flatFileRepository.setMetadata(BasicPoly.newPoly().withData(flatFile.metadata()));
                flatFileRepository.setConfig(flatFile.toPoly());
            } catch (IOException e) {
                log.error("Failed to load poly {}", polyDir.getName(), e);
            }
        }

        // scan for YAMLs and load to poly
        for (File file : FileUtils.listFiles(new File(polyDir, DATA_DIR), POLY_EXTENSIONS, true)) {
            log.info("Loading file {}", file.getPath());
            try {
                FlatFile flatFile = MAPPER.readValue(file, FlatFile.class);
                BasicPoly poly = flatFile.toPoly();
                List<String> index = null;
                if (flatFile.metadata() != null) {
                    index = flatFile.metadata().getIndex();
                }
                if (index == null) {
                    index = new ArrayList<>();
                }
                index.add(DATE_INDEX);
                flatFileRepository.add(poly, index);
            } catch (IOException e) {
                log.error("Failed to load file {}", file.getName(), e);
            }

        }

        repositories.put(polyDir.getName(), flatFileRepository);
    }

    @Override
    public BasicPoly create(String poly) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public boolean exists(String poly) {
        return repositories.containsKey(poly);
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return Optional.ofNullable(repositories.get(poly).getConfig());
    }

    @Override
    public void config(String poly, BasicPoly config) {
        throw new UnsupportedOperationException("Operation not supported");

    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return Optional.ofNullable(repositories.get(poly).getMetadata());
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {
        throw new UnsupportedOperationException("Operation not supported");

    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        BasicPoly index = new BasicPoly();
        repositories.get(poly).getPolyIndex().forEach((key, value) -> index.put(key, BasicPoly.newPoly(key).with("_count", value.size())));
        return Optional.of(index);
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return index(poly).map(p -> p.fetch(indexId));
    }

    @Override
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList update(String poly, Collection<PersistRequest> persistRequests) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        if (!exists(poly)) {
            return new BasicPolyList();
        }
        return repositories.get(poly).fetchById(ids);
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList query(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public BasicPolyList list() {
        BasicPolyList list = new BasicPolyList();
        repositories.keySet().forEach(poly -> list.add(BasicPoly.newPoly(poly)));
        return list;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }
}
