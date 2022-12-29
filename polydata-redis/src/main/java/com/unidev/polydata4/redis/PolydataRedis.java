package com.unidev.polydata4.redis;

import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.api.packer.PolyPacker;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.PersistRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Polydata storage in Redis.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataRedis extends AbstractPolydata {

    private static final String POLY_LIST = "poly_list";

    // prefix key
    @Getter
    private final String prefix = "";

    @Getter
    private final JedisPool pool;

    @Getter
    private final PolyPacker polyPacker;

    @Getter
    private boolean hashIds = false;

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String poly) {
        if (exists(poly)) {
            return config(poly).get();
        }
        BasicPoly config = new BasicPoly();
        config._id(poly + "-" + CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(poly, config);
        metadata(poly, BasicPoly.newPoly(poly + "-" + METADATA_KEY));

        try (Jedis jedis = pool.getResource()) {
            jedis.lpush(POLY_LIST, poly);
        }

        return config(poly).get();
    }

    @Override
    public boolean exists(String poly) {
        return config(poly).isPresent();
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        try (Jedis jedis = pool.getResource()) {
            byte[] value = jedis.get(fetchId(poly, CONFIG_KEY));
            if (value == null || value.length == 0) {
                return Optional.empty();
            }
            try {
                return Optional.of(polyPacker.unPackPoly(new ByteArrayInputStream(value)));
            } catch (Exception e) {
                log.error("Failed to fetch config", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void config(String poly, BasicPoly config) {

    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        return Optional.empty();
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {

    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        return Optional.empty();
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        return Optional.empty();
    }

    @Override
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        return null;
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
        return null;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    private byte[] fetchId(String poly, String id) {
        String value = prefix + poly + "-" + id;
        if (!hashIds) {
            return (value.hashCode()+ "").getBytes();
        }
        return DigestUtils.sha256Hex(value).toLowerCase().getBytes();
    }
}
