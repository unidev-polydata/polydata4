package com.unidev.polydata4.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * List of basic poly records
 */
@ToString
public class BasicPolyList implements PolyList<BasicPoly>, Serializable {

    @Getter
    @Setter
    protected List<BasicPoly> list;

    @Getter
    @Setter
    protected Map<String, Object> metadata;

    public BasicPolyList(Map<String, Object> metadata, List<BasicPoly> list) {
        this.list = list;
        this.metadata = metadata;
    }

    public BasicPolyList() {
        this(new ConcurrentHashMap<>(), new CopyOnWriteArrayList<>());
    }

    public BasicPolyList(Collection<BasicPoly> source) {
        this(new ConcurrentHashMap<>(), new CopyOnWriteArrayList<>(source));
    }

    public static BasicPolyList newList() {
        return new BasicPolyList();
    }

    public static BasicPolyList newList(Collection<BasicPoly> source) {
        return new BasicPolyList(source);
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public PolyList<BasicPoly> withMetadata(Map<String, Object> meta) {
        this.metadata = meta;
        return this;
    }

    @Override
    public List<BasicPoly> list() {
        return list;
    }

    @Override
    public List<BasicPoly> getList() {
        return list;
    }

    @Override
    public PolyList<BasicPoly> withList(List<BasicPoly> polys) {
        this.list = polys;
        return this;
    }

    @Override
    public Optional<BasicPoly> polyById(String id) {
        if (list == null) {
            return Optional.empty();
        }
        for (BasicPoly basicPoly : list) {
            if (basicPoly._id().equals(id)) {
                return Optional.of(basicPoly);
            }
        }
        return Optional.empty();
    }

    @Override
    public BasicPoly safeById(String id) {
        if (list == null) {
            return null;
        }
        for (BasicPoly basicPoly : list) {
            if (basicPoly._id().equals(id)) {
                return basicPoly;
            }
        }
        return null;
    }

    @Override
    public <P extends Poly> PolyList<BasicPoly> add(P poly) {
        list.add((BasicPoly) poly);
        return this;
    }

    @Override
    public PolyList<BasicPoly> delete(String polyId) {
        BasicPoly toDelete = null;
        for (BasicPoly poly : list) {
            if (poly._id().equals(polyId)) {
                toDelete = poly;
                break;
            }
        }
        list.remove(toDelete);
        return this;
    }

    @Override
    public boolean hasPoly(String polyId) {
        return polyById(polyId).isPresent();
    }

}
