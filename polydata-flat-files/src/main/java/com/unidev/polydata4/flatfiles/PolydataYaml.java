package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.unidev.platform.Randoms;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

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
    public static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private static final String[] POLY_EXTENSIONS = new String[]{"yaml", "yml"};

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
    private final Randoms randoms = new Randoms();

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
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
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
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(poly);

        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + poly + " is not configured");
        }

        String index = DATE_INDEX;
        String queryIndex = query.index();
        if (!StringUtils.isBlank(queryIndex)) {
            index = queryIndex;
        }
        BasicPoly config = configPoly.get();

        Integer defaultItemPerPage = config.fetch(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        Integer itemPerPage = query.getOptions().fetch(ITEM_PER_PAGE, defaultItemPerPage);

        if (query.queryType() == BasicPolyQuery.QueryFunction.RANDOM) {
            List<String> indexes = repositories.get(poly).getPolyIndex().get(index);
            List<String> randomIds = randoms.randomValues(indexes, itemPerPage);
            return read(poly, new HashSet<>(randomIds));
        }
        final int page = query.page() < 0 ? 0 : query.page();
        List<Integer> ids = new ArrayList<>();
        for (int i = page * itemPerPage; i < (page + 1) * itemPerPage; i++) {
            ids.add(i);
        }

        return repositories.get(poly).fetchIndexById(index, ids);
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(poly);
        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + poly + " is not configured");
        }
        String index = DATE_INDEX;
        String queryIndex = query.index();
        if (!StringUtils.isBlank(queryIndex)) {
            index = queryIndex;
        }
        List<String> ids = repositories.get(poly).getPolyIndex().get(index);
        if (CollectionUtils.isEmpty(ids)) {
            return 0L;
        }
        return (long) ids.size();
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
