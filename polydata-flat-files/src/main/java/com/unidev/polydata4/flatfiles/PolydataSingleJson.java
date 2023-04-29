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
        indexToPersist.add(DATE_INDEX);
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
    public BasicPoly create(String poly) {
        Optional<BasicPoly> existingConfig = config(poly);
        if (existingConfig.isPresent()) {
            return existingConfig.get();
        }

        FlatFileRepository repository = new FlatFileRepository();
        repositories.put(poly, repository);

        BasicPoly config = new BasicPoly();
        config._id(CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(poly, config);
        metadata(poly, BasicPoly.newPoly(METADATA_KEY));

        return config(poly).get();
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
        if (!exists(poly)) {
            throw new RuntimeException("Poly " + poly + " does not exists");
        }
        repositories.get(poly).setConfig(config);
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
        if (!exists(poly)) {
            throw new RuntimeException("Poly " + poly + " does not exists");
        }
        repositories.get(poly).setMetadata(metadata);
    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        BasicPoly index = new BasicPoly();
        repositories.get(poly).getPolyIndex().forEach((key, value) -> index.put(key, BasicPoly.newPoly(key).with("count", value.size())));
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
        BasicPolyList list = new BasicPolyList();
        FlatFileRepository repository = repositories.get(poly);
        insertRequests.forEach(request -> {
            BasicPoly data = request.getData();
            String id = data._id();
            repository.remove(id);
            Set<String> tags = buildTagIndex(request);
            repository.add(data, tags);
            repository.fetchById(Set.of(id)).polyById(id).ifPresent(list::add);
        });
        return list;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        return insert(poly, insertRequests);
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        FlatFileRepository repository = repositories.get(poly);
        return repository.fetchById(ids);
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        FlatFileRepository repository = repositories.get(poly);
        BasicPolyList list = read(poly, ids);
        ids.forEach(id -> {
            repository.remove(id);
        });
        return list;
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
            int randomCount = query.option(RANDOM_COUNT, itemPerPage);
            List<String> randomIds = randoms.randomValues(indexes, randomCount);
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
