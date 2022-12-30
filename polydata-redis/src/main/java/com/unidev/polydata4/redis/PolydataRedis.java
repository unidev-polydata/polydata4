package com.unidev.polydata4.redis;

import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.api.packer.PolyPacker;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.PersistRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Polydata storage in Redis.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataRedis extends AbstractPolydata {

    @RequiredArgsConstructor
    @Builder
    public static class PolydataRedisConfig {
        final String prefix = "";

        final JedisPool pool;

        final PolyPacker polyPacker;

        final boolean hashIds;

    }

    static final String POLY_LIST = "poly-list";

    static final String TAG_INDEX_KEY = "tag-index";

    private final PolydataRedisConfig polyConfig;

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
        return config(poly).isPresent();
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        if (!exists(poly)) {
            return Optional.empty();
        }
        return redis(jedis -> {
            return readPoly(jedis, poly,  CONFIG_KEY);
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
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        final BasicPolyList basicPolyList = new BasicPolyList();
        redis(jedis -> {
            for (PersistRequest persistRequest : persistRequests) {

                BasicPoly polyToPersist = persistRequest.getPoly();
                writePoly(jedis, poly, polyToPersist);
                Map<String, BasicPoly> indexData = persistRequest.getIndexData();

                for (String indexName : persistRequest.getIndexToPersist()) {
                    // add poly it to list of polys
                    byte[] indexId = fetchId(poly, indexName);
                    jedis.lpush(indexId, indexId);

                    BasicPoly tagIndex = index(poly).orElseGet(() -> BasicPoly.newPoly());

                    // update tags list
                    Map data = new HashMap();
                    if (indexData != null) {
                        data = indexData.getOrDefault(indexName, BasicPoly.newPoly()).data();
                    }

                    data.put("_count", jedis.llen(indexId));
                    tagIndex.put(indexName, data);
                    writePoly(jedis, poly, tagIndex);
                }
            }
        });

        return basicPolyList;
    }

    @Override
    public BasicPolyList update(String poly, Collection<PersistRequest> persistRequests) {
        return null;
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        return null;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        return null;
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
}
