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
    Optional<BasicPoly> index(String poly);

    /** Return index information **/
    BasicPoly indexData(String poly, String indexId);

    BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests);

    BasicPolyList update(String poly, Collection<PersistRequest> persistRequests);

    BasicPolyList read(String poly, Set<String> ids);

    BasicPolyList remove(String poly, Set<String> ids);

    BasicPolyList query(String poly, BasicPolyQuery polyQuery);

    Long count(String poly, BasicPolyQuery polyQuery);

    /**
     * List available polys
     */
    BasicPolyList list();

    void open();

    void close() throws IOException;

}
