package com.unidev.polydata4.domain;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Ordered list of polys
 *
 * @param <T>
 */
public interface PolyList<T extends Poly> extends Serializable {


    Map<String, Object> getMetadata();

    Map<String, Object> metadata();

    PolyList<T> withMetadata(Map<String, Object> meta);

    List<T> list();

    List<T> getList();

    PolyList<T> withList(List<T> poly);

    /**
     * Greedy search of polys in the list
     */
    Optional<T> polyById(String id);

    T safeById(String id);

    <P extends Poly> PolyList<T> add(P poly);

    <P extends Poly> PolyList<T> add(PolyList<T> list);

    PolyList<T> delete(String polyId);

    boolean hasPoly(String polyId);

}
