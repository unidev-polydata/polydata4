package com.unidev.polydata4.domain;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * List of basic poly records
 */
public class BasicPolyMap implements PolyMap<BasicPoly>, Serializable {

    @Getter
    @Setter
    private Map<String, BasicPoly> map;

    @Getter
    @Setter
    private Map<String, Object> metadata;

    public BasicPolyMap() {
        super();
        map = new ConcurrentHashMap<>();
        metadata = new ConcurrentHashMap<>();
    }

    public static BasicPolyMap basicPolyMap(BasicPoly poly) {
        BasicPolyMap map = new BasicPolyMap();
        map.put(poly);
        return map;
    }

    public static BasicPolyMap basicPolyMap() {
        return new BasicPolyMap();
    }

    @Override
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public PolyMap<BasicPoly> withMetadata(Map<String, Object> meta) {
        metadata = meta;
        return this;
    }

    @Override
    public Map<String, BasicPoly> map() {
        return map;
    }

    @Override
    public PolyMap<BasicPoly> withMap(Map<String, BasicPoly> map) {
        this.map = map;
        return this;
    }

    @Override
    public Optional<BasicPoly> polyById(String id) {
        if (map == null) {
            return Optional.empty();
        }
        if (map.containsKey(id)) {
            return Optional.of(map.get(id));
        }
        return Optional.empty();
    }

    public BasicPolyMap polyByIds(Collection<String> ids) {
        BasicPolyMap result = new BasicPolyMap();

        for (String id : ids) {
            if (map.containsKey(id)) {
                result.put(map.get(id));
            }
        }

        return result;
    }

    @Override
    public <P extends Poly> PolyMap<BasicPoly> put(P poly) {
        map.put(poly._id(), (BasicPoly) poly);
        return this;
    }

    @Override
    public <P extends Poly> PolyMap<BasicPoly> deletePoly(String polyId) {
        map.remove(polyId);
        return this;
    }

    @Override
    public boolean hasPoly(String polyId) {
        return polyById(polyId).isPresent();
    }

    @Override
    public String toString() {
        return "BasicPolyMap{" +
                "map=" + map +
                ", metadata=" + metadata +
                '}';
    }
}
