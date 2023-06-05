package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Polydata storage backed by Yaml files. Read only storage.
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
        FlatFileDeserializer.install(MAPPER);
    }

    @Getter
    private final File rootDir;

    // poly -> repository of items
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
                flatFileRepository.add(poly, index);
            } catch (IOException e) {
                log.error("Failed to load file {}", file.getName(), e);
            }

        }

        repositories.put(polyDir.getName(), flatFileRepository);
    }

    @Override
    public BasicPoly create(String dataset) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public boolean exists(String dataset) {
        return repositories.containsKey(dataset);
    }

    @Override
    public Optional<BasicPoly> config(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return Optional.ofNullable(repositories.get(dataset).getConfig());
    }

    @Override
    public void config(String dataset, BasicPoly config) {
        throw new UnsupportedOperationException("Operation not supported");

    }

    @Override
    public Optional<BasicPoly> metadata(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return Optional.ofNullable(repositories.get(dataset).getMetadata());
    }

    @Override
    public void metadata(String dataset, BasicPoly metadata) {
        throw new UnsupportedOperationException("Operation not supported");

    }

    @Override
    public Optional<BasicPoly> index(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        BasicPoly index = new BasicPoly();
        repositories.get(dataset).getPolyIndex().forEach((key, value) -> index.put(key, BasicPoly.newPoly(key).with("_count", value.size())));
        return Optional.of(index);
    }

    @Override
    public Optional<BasicPoly> indexData(String dataset, String indexId) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return index(dataset).map(p -> p.fetch(indexId));
    }

    @Override
    public BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList update(String dataset, Collection<InsertRequest> insertRequests) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList read(String dataset, Set<String> ids) {
        if (!exists(dataset)) {
            return new BasicPolyList();
        }
        return repositories.get(dataset).fetchById(ids);
    }

    @Override
    public BasicPolyList remove(String dataset, Set<String> ids) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public BasicPolyList query(String dataset, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(dataset);

        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + dataset + " is not configured");
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
            List<String> indexes = repositories.get(dataset).getPolyIndex().get(index);
            int randomCount = query.option(RANDOM_COUNT, itemPerPage);
            List<String> randomIds = randoms.randomValues(indexes, randomCount);
            return read(dataset, new HashSet<>(randomIds));
        }
        final int page = query.page() < 0 ? 0 : query.page();
        List<Integer> ids = new ArrayList<>();
        for (int i = page * itemPerPage; i < (page + 1) * itemPerPage; i++) {
            ids.add(i);
        }

        return repositories.get(dataset).fetchIndexById(index, ids);
    }

    @Override
    public Long count(String dataset, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(dataset);
        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + dataset + " is not configured");
        }
        String index = DATE_INDEX;
        String queryIndex = query.index();
        if (!StringUtils.isBlank(queryIndex)) {
            index = queryIndex;
        }
        List<String> ids = repositories.get(dataset).getPolyIndex().get(index);
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
