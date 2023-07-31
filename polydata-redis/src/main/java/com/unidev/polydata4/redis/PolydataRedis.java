package com.unidev.polydata4.redis;

import com.unidev.platform.Randoms;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.api.packer.PolyPacker;
import com.unidev.polydata4.domain.*;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Polydata storage in Redis.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataRedis extends AbstractPolydata {

    static final String POLY_LIST = "poly-list";
    static final String TAG_INDEX_KEY = "tag-index";
    private final PolydataRedisConfig polyConfig;
    private final Randoms randoms = new Randoms();

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String dataset) {
        if (exists(dataset)) {
            return config(dataset).get();
        }
        BasicPoly config = new BasicPoly();
        config._id(CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(dataset, config);
        metadata(dataset, BasicPoly.newPoly(METADATA_KEY));
        redis(jedis -> {
            jedis.lpush(POLY_LIST, dataset);
        });
        return config(dataset).get();
    }

    @Override
    public boolean exists(String dataset) {
        return list().hasPoly(dataset);
    }

    @Override
    public Optional<BasicPoly> config(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, dataset, CONFIG_KEY);
        });
    }

    @Override
    public void config(String dataset, BasicPoly config) {
        redis(jedis -> {
            writePoly(jedis, dataset, config);
        });
    }

    @Override
    public Optional<BasicPoly> metadata(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, dataset, METADATA_KEY);
        });
    }

    @Override
    public void metadata(String dataset, BasicPoly metadata) {
        redis(jedis -> {
            writePoly(jedis, dataset, metadata);
        });
    }

    @Override
    public Optional<BasicPoly> index(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, dataset, TAG_INDEX_KEY);
        });
    }

    @Override
    public Optional<BasicPoly> indexData(String dataset, String indexId) {
        Optional<BasicPoly> index = index(dataset);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        return index.get().fetch(indexId);
    }

    @Override
    public BasicPolyList insert(String dataset, InsertOptions insertOptions, Collection<InsertRequest> insertRequests) {
        final BasicPolyList basicPolyList = new BasicPolyList();
        redis(jedis -> {
            // build index data
            for (InsertRequest insertRequest : insertRequests) {
                Set<String> indexToPersist = insertRequest.getIndexToPersist();
                if (CollectionUtils.isEmpty(indexToPersist)) {
                    indexToPersist = new HashSet<>();
                } else {
                    indexToPersist = new HashSet<>(indexToPersist);
                }
                insertRequest.setIndexToPersist(indexToPersist);
                insertRequest.getData().put(INDEXES, indexToPersist);
            }

            for (InsertRequest insertRequest : insertRequests) {
                BasicPoly polyToPersist = insertRequest.getData();
                writePoly(jedis, dataset, polyToPersist);
                removePolyFromIndex(jedis, dataset, polyToPersist);
                for (String indexName : insertRequest.getIndexToPersist()) {
                    // add poly it to list of polys
                    byte[] indexId = fetchIndexId(dataset, indexName);
                    jedis.lpush(indexId, insertRequest.getData()._id().getBytes());
                }
            }
            if (insertOptions.skipIndex()) {
                rebuildIndex(jedis, dataset);
            }
        });

        return basicPolyList;
    }

    @Override
    public BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, InsertOptions.defaultInsertOptions(), insertRequests);
    }

    private void rebuildIndex(Jedis jedis, String dataset) {
        byte[] pattern = fetchIndexId(dataset, "*");
        Set<byte[]> keys = jedis.keys(pattern);
        BasicPoly tagIndex = BasicPoly.newPoly(TAG_INDEX_KEY);
        for (byte[] key : keys) {
            String stringKey = StringUtils.replace(new String(key), new String(fetchIndexId(dataset, "")), "");
            long length = jedis.llen(key);
            tagIndex.put(stringKey, BasicPoly.newPoly().with("count", length));
        }
        writePoly(jedis, dataset, tagIndex);
    }

    private void removePolyFromIndex(Jedis jedis, String dataset, BasicPoly p) {
        byte[] pattern = fetchIndexId(dataset, "*");
        Set<byte[]> keys = jedis.keys(pattern);
        for (byte[] index : keys) {
            jedis.lrem(index, 0, p._id().getBytes());
        }
    }

    @Override
    public BasicPolyList update(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, insertRequests);
    }

    @Override
    public BasicPolyList read(String dataset, Set<String> ids) {
        return redis(jedis -> {
            BasicPolyList basicPolyList = new BasicPolyList();
            jedis.mget(
                    ids.stream().map(id -> fetchId(dataset, id)).toArray(byte[][]::new)
            ).forEach(polyData -> {
                if (polyData == null) {
                    return;
                }
                try {
                    BasicPoly basicPoly = polyConfig.polyPacker.unPackPoly(new ByteArrayInputStream(polyData));
                    basicPolyList.add(basicPoly);
                } catch (Exception e) {
                    log.error("Failed to unpack poly", e);
                }
            });
            return basicPolyList;
        });
    }

    @Override
    public BasicPolyList remove(String dataset, Set<String> ids) {
        return redis(jedis -> {
            byte[][] redisIds = ids.stream().map(id -> fetchId(dataset, id)).toArray(byte[][]::new);
            BasicPolyList basicPolyList = read(dataset, ids);
            // remove ids
            for (byte[] id : redisIds) {
                try {
                    jedis.del(id);
                } catch (Exception e) {
                    log.error("Failed to delete poly", e);
                }
            }
            // remove poly from indexes
            for (BasicPoly p : basicPolyList.list()) {
                Collection<String> indexes = p.fetch(INDEXES);
                for (String index : indexes) {
                    byte[] indexId = fetchIndexId(dataset, index);
                    jedis.lrem(indexId, 0, p._id().getBytes());
                }
            }
            rebuildIndex(jedis, dataset);
            return basicPolyList;
        });
    }

    @Override
    public BasicPolyList query(String dataset, PolyQuery polyQuery) {
        return redis(jedis -> {
            BasicPolyQuery query = (BasicPolyQuery) polyQuery;
            Optional<BasicPoly> configPoly = config(dataset);

            if (configPoly.isEmpty()) {
                throw new RuntimeException("Poly " + dataset + " is not configured");
            }

            String index;
            String queryIndex = query.index();
            if (!StringUtils.isBlank(queryIndex)) {
                index = queryIndex;
            } else {
                index = DATE_INDEX;
            }
            BasicPoly config = configPoly.get();

            Integer defaultItemPerPage = config.fetch(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
            Integer itemPerPage = query.getOptions().fetch(ITEM_PER_PAGE, defaultItemPerPage);

            List<Integer> ids = new ArrayList<>();
            if (query.queryType() == BasicPolyQuery.QueryFunction.RANDOM) {
                long count = jedis.llen(fetchIndexId(dataset, index));
                int randomCount = query.option(RANDOM_COUNT, itemPerPage);

                for (int i = 0; i < randomCount; i++) {
                    ids.add(randoms.getRandom().nextInt((int) count));
                }
            } else {
                final int page = query.page() < 0 ? 0 : query.page();
                for (int i = page * itemPerPage; i < (page + 1) * itemPerPage; i++) {
                    ids.add(i);
                }
            }

            Set<String> indexIds = ids.stream()
                    .map(id -> jedis.lindex(fetchIndexId(dataset, index), id))
                    .filter(Objects::nonNull)
                    .map(id -> new String(id))
                    .collect(Collectors.toSet());

            return read(dataset, indexIds);
        });
    }

    @Override
    public Long count(String dataset, PolyQuery polyQuery) {
        return redis(jedis -> {
            BasicPolyQuery query = (BasicPolyQuery) polyQuery;
            Optional<BasicPoly> configPoly = config(dataset);
            if (configPoly.isEmpty()) {
                throw new RuntimeException("Poly " + dataset + " is not configured");
            }
            String index;
            String queryIndex = query.index();
            if (!StringUtils.isBlank(queryIndex)) {
                index = queryIndex;
            } else {
                index = DATE_INDEX;
            }
            byte[] indexId = fetchIndexId(dataset, index);
            return jedis.llen(indexId);
        });
    }

    @Override
    public BasicPolyList list() {
        return redis(jedis -> {
            BasicPolyList polyList = new BasicPolyList();
            for (String dataset : jedis.lrange(POLY_LIST, 0, -1)) {
                polyList.add(BasicPoly.newPoly(dataset));
            }
            return polyList;
        });
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    private byte[] fetchId(String dataset, String id) {
        String value = polyConfig.prefix + dataset + "-" + id;
        if (!polyConfig.hashIds) {
            return value.getBytes();
        }
        return DigestUtils.sha256Hex(value).toLowerCase().getBytes();
    }

    private byte[] fetchIndexId(String dataset, String id) {
        String value = polyConfig.prefix + dataset + "-index-" + id;
        if (!polyConfig.hashIds) {
            return value.getBytes();
        }
        return DigestUtils.sha256Hex(value).toLowerCase().getBytes();
    }

    /**
     * Execute logic on redis connection.
     */
    private <R> R redis(Function<Jedis, R> logic) {
        try (Jedis jedis = polyConfig.pool.getResource()) {
            return logic.apply(jedis);
        }
    }

    /**
     * Execute logic on redis connection.
     */
    private void redis(Consumer<Jedis> logic) {
        try (Jedis jedis = polyConfig.pool.getResource()) {
            logic.accept(jedis);
        }
    }

    /**
     * Write poly to redis.
     */
    public void writePoly(Jedis jedis, String dataset, BasicPoly data) {
        try {
            byte[] configBytes = polyConfig.polyPacker.packPoly(data);
            jedis.set(fetchId(dataset, data._id()), configBytes);
        } catch (Exception e) {
            log.error("Failed to persist config", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Read raw poly from redis.
     */
    public Optional<BasicPoly> readPoly(Jedis jedis, String dataset, String id) {
        byte[] value = jedis.get(fetchId(dataset, id));
        if (value == null || value.length == 0) {
            return Optional.empty();
        }
        try {
            return Optional.of(polyConfig.polyPacker.unPackPoly(new ByteArrayInputStream(value)));
        } catch (Exception e) {
            log.error("Failed to fetch poly", e);
            throw new RuntimeException(e);
        }
    }

    @RequiredArgsConstructor
    @Builder
    public static class PolydataRedisConfig {
        final String prefix;

        final JedisPool pool;

        final PolyPacker polyPacker;

        final boolean hashIds;

    }
}
