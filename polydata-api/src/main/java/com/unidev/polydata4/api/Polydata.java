package com.unidev.polydata4.api;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.PersistRequest;
import com.unidev.polydata4.domain.BasicPolyQuery;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Storage interface for polydata records.
 */
public interface Polydata extends Closeable {

    BasicPoly create(String poly);

    /**
     * Check if exists poly storage.
     * @param poly
     * @return
     */
    boolean exists(String poly);

    Optional<BasicPoly> config(String poly);

    void config(String poly, BasicPoly config);

    Optional<BasicPoly> metadata(String poly);

    void metadata(String poly, BasicPoly metadata);

    /**
     * Fetch tags from specific index.
     * Examples:
     *  Index: tags
     *  Values: dogs: 10, cats: 2
     *
     *  Index: _date
     *  Values: _count: 1000
     */
    Optional<BasicPoly> fetchIndexes(String poly);

    /** Return tag information **/
    BasicPoly fetchIndexData(String poly, String indexId);

    BasicPolyList persistPoly(String poly, Collection<PersistRequest> persistRequests);

    BasicPolyList updatePoly(String poly,Collection<PersistRequest> persistRequests);

    BasicPolyList readPoly(String poly, Set<String> ids);

    BasicPolyList removePoly(String poly, Set<String> ids);

    BasicPolyList query(String poly, BasicPolyQuery polyQuery);

    /**
     * List available polys
     */
    BasicPolyList listPolys();

    void open();

    void close() throws IOException;

}
