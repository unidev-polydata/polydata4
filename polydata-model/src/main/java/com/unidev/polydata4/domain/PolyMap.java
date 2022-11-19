package com.unidev.polydata4.domain;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Map of polys.
 *
 * @param <T>
 */
public interface PolyMap<T extends Poly> extends Serializable {

    Map<String, Object> metadata();

    PolyMap<T> withMetadata(Map<String, Object> meta);

    Map<String, T> map();

    PolyMap<T> withMap(Map<String, T> map);

    /**
     * Get poly by id
     */
    Optional<T> polyById(String id);

    <P extends Poly> PolyMap<T> put(P poly);

    <P extends Poly> PolyMap<T> deletePoly(String polyId);

    boolean hasPoly(String polyId);
}


