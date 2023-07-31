package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.platform.Randoms;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polydata storage in single JSON file
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataSingleJson extends AbstractPolydata {

    public static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] POLY_EXTENSIONS = new String[]{"poly.json"};

    @Getter
    private final File rootDir;

    // poly -> repository of items
    @Getter
    private final Map<String, FlatFileRepository> repositories = new ConcurrentHashMap<>();
    private final Randoms randoms = new Randoms();

    private static Set<String> buildTagIndex(InsertRequest request) {
        Set<String> indexToPersist = request.getIndexToPersist();
        if (CollectionUtils.isEmpty(indexToPersist)) {
            indexToPersist = new HashSet<>();
        } else {
            indexToPersist = new HashSet<>(indexToPersist);
        }
        return indexToPersist;
    }

    @Override
    public void prepareStorage() {
        FileUtils.listFiles(rootDir, POLY_EXTENSIONS, true).forEach(file -> {
            log.info("Loading {}", file.getName());
            try {
                FlatFileRepository repository = MAPPER.readValue(file, FlatFileRepository.class);
                String key = FilenameUtils.getBaseName(file.getName());
                repositories.put(key, repository);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    @Override
    public BasicPoly create(String dataset) {
        Optional<BasicPoly> existingConfig = config(dataset);
        if (existingConfig.isPresent()) {
            return existingConfig.get();
        }

        FlatFileRepository repository = new FlatFileRepository();
        repositories.put(dataset, repository);

        BasicPoly config = new BasicPoly();
        config._id(CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(dataset, config);
        metadata(dataset, BasicPoly.newPoly(METADATA_KEY));

        return config(dataset).get();
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
        if (!exists(dataset)) {
            throw new RuntimeException("Poly " + dataset + " does not exists");
        }
        repositories.get(dataset).setConfig(config);
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
        if (!exists(dataset)) {
            throw new RuntimeException("Poly " + dataset + " does not exists");
        }
        repositories.get(dataset).setMetadata(metadata);
    }

    @Override
    public Optional<BasicPoly> index(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        BasicPoly index = new BasicPoly();
        repositories.get(dataset).getPolyIndex().forEach((key, value) -> index.put(key, BasicPoly.newPoly(key).with("count", value.size())));
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
    public BasicPolyList insert(String dataset, InsertOptions insertOptions, Collection<InsertRequest> insertRequests) {
        BasicPolyList list = new BasicPolyList();
        FlatFileRepository repository = repositories.get(dataset);
        insertRequests.forEach(request -> {
            BasicPoly data = request.getData();
            String id = data._id();
            repository.remove(id);
            repository.fetchById(Set.of(id)).polyById(id).ifPresent(list::add);
            if (!insertOptions.skipIndex()) {
                Set<String> tags = buildTagIndex(request);
                repository.add(data, tags);
            }
        });
        return list;
    }

    @Override
    public BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, InsertOptions.defaultInsertOptions(), insertRequests);
    }

    @Override
    public BasicPolyList update(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, insertRequests);
    }

    @Override
    public BasicPolyList read(String dataset, Set<String> ids) {
        FlatFileRepository repository = repositories.get(dataset);
        return repository.fetchById(ids);
    }

    @Override
    public BasicPolyList remove(String dataset, Set<String> ids) {
        FlatFileRepository repository = repositories.get(dataset);
        BasicPolyList list = read(dataset, ids);
        ids.forEach(id -> {
            repository.remove(id);
        });
        return list;
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
        repositories.keySet().forEach(key -> list.add(BasicPoly.newPoly(key)));
        return list;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {
        repositories.entrySet().forEach(entry -> {
            String key = entry.getKey();
            FlatFileRepository repository = entry.getValue();
            File file = new File(rootDir, key + ".poly.json");
            try {
                MAPPER.writeValue(file, repository);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
