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
    public BasicPoly create(String poly) {
        if (exists(poly)) {
            return config(poly).get();
        }
        BasicPoly config = new BasicPoly();
        config._id(CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(poly, config);
        metadata(poly, BasicPoly.newPoly(METADATA_KEY));
        redis(jedis -> {
            jedis.lpush(POLY_LIST, poly);
        });
        return config(poly).get();
    }

    @Override
    public boolean exists(String poly) {
        return list().hasPoly(poly);
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, poly, CONFIG_KEY);
        });
    }

    @Override
    public void config(String poly, BasicPoly config) {
        redis(jedis -> {
            writePoly(jedis, poly, config);
        });
    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, poly, METADATA_KEY);
        });
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {
        redis(jedis -> {
            writePoly(jedis, poly, metadata);
        });
    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, poly, TAG_INDEX_KEY);
        });
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        Optional<BasicPoly> index = index(poly);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        return index.get().fetch(indexId);
    }

    @Override
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
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
                indexToPersist.add(DATE_INDEX);
                insertRequest.setIndexToPersist(indexToPersist);
                insertRequest.getData().put(INDEXES, indexToPersist);
            }

            for (InsertRequest insertRequest : insertRequests) {
                BasicPoly polyToPersist = insertRequest.getData();
                writePoly(jedis, poly, polyToPersist);
                removePolyFromIndex(jedis, poly, polyToPersist);
                for (String indexName : insertRequest.getIndexToPersist()) {
                    // add poly it to list of polys
                    byte[] indexId = fetchIndexId(poly, indexName);
                    jedis.lpush(indexId, insertRequest.getData()._id().getBytes());
                }
            }
            rebuildIndex(jedis, poly);
        });

        return basicPolyList;
    }

    private void rebuildIndex(Jedis jedis, String poly) {
        byte[] pattern = fetchIndexId(poly, "*");
        Set<byte[]> keys = jedis.keys(pattern);
        BasicPoly tagIndex = BasicPoly.newPoly(TAG_INDEX_KEY);
        for (byte[] key : keys) {
            String stringKey = StringUtils.replace(new String(key), new String(fetchIndexId(poly, "")), "");
            long length = jedis.llen(key);
            tagIndex.put(stringKey, BasicPoly.newPoly().with("count", length));
        }
        writePoly(jedis, poly, tagIndex);
    }

    private void removePolyFromIndex(Jedis jedis, String poly, BasicPoly p) {
        byte[] pattern = fetchIndexId(poly, "*");
        Set<byte[]> keys = jedis.keys(pattern);
        for (byte[] index : keys) {
            jedis.lrem(index, 0, p._id().getBytes());
        }
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        return insert(poly, insertRequests);
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        return redis(jedis -> {
            BasicPolyList basicPolyList = new BasicPolyList();
            jedis.mget(
                    ids.stream().map(id -> fetchId(poly, id)).toArray(byte[][]::new)
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
    public BasicPolyList remove(String poly, Set<String> ids) {
        return redis(jedis -> {
            byte[][] redisIds = ids.stream().map(id -> fetchId(poly, id)).toArray(byte[][]::new);
            BasicPolyList basicPolyList = read(poly, ids);
            // remove ids
            for (byte[] id : redisIds) {
                try {
                    jedis.del(id);
                } catch (Exception e) {
                    log.error("Failed to delete poly", e);
                }
            }
            // remove poly from indexes
            for(BasicPoly p: basicPolyList.list()) {
                Collection<String> indexes = p.fetch(INDEXES);
                for (String index : indexes) {
                    byte[] indexId = fetchIndexId(poly, index);
                    jedis.lrem(indexId, 0, p._id().getBytes());
                }
            }
            rebuildIndex(jedis, poly);
            return basicPolyList;
        });
    }

    @Override
    public BasicPolyList query(String poly, PolyQuery polyQuery) {
        return redis(jedis -> {
            BasicPolyQuery query = (BasicPolyQuery) polyQuery;
            Optional<BasicPoly> configPoly = config(poly);

            if (configPoly.isEmpty()) {
                throw new RuntimeException("Poly " + poly + " is not configured");
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
                long count = jedis.llen(fetchIndexId(poly, index));
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
                    .map(id -> jedis.lindex(fetchIndexId(poly, index), id))
                    .filter(Objects::nonNull)
                    .map(id -> new String(id))
                    .collect(Collectors.toSet());

            return read(poly, indexIds);
        });
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        return redis(jedis -> {
            BasicPolyQuery query = (BasicPolyQuery) polyQuery;
            Optional<BasicPoly> configPoly = config(poly);
            if (configPoly.isEmpty()) {
                throw new RuntimeException("Poly " + poly + " is not configured");
            }
            String index;
            String queryIndex = query.index();
            if (!StringUtils.isBlank(queryIndex)) {
                index = queryIndex;
            } else {
                index = DATE_INDEX;
            }
            byte[] indexId = fetchIndexId(poly, index);
            return jedis.llen(indexId);
        });
    }

    @Override
    public BasicPolyList list() {
        return redis(jedis -> {
            BasicPolyList polyList = new BasicPolyList();
            for (String poly : jedis.lrange(POLY_LIST, 0, -1)) {
                polyList.add(BasicPoly.newPoly(poly));
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

    private byte[] fetchId(String poly, String id) {
        String value = polyConfig.prefix + poly + "-" + id;
        if (!polyConfig.hashIds) {
            return value.getBytes();
        }
        return DigestUtils.sha256Hex(value).toLowerCase().getBytes();
    }

    private byte[] fetchIndexId(String poly, String id) {
        String value = polyConfig.prefix + poly + "-index-" + id;
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
    public void writePoly(Jedis jedis, String poly, BasicPoly data) {
        try {
            byte[] configBytes = polyConfig.polyPacker.packPoly(data);
            jedis.set(fetchId(poly, data._id()), configBytes);
        } catch (Exception e) {
            log.error("Failed to persist config", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Read raw poly from redis.
     */
    public Optional<BasicPoly> readPoly(Jedis jedis, String poly, String id) {
        byte[] value = jedis.get(fetchId(poly, id));
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
