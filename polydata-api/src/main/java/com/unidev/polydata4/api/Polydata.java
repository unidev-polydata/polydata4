package com.unidev.polydata4.api;

import com.unidev.polydata4.domain.BasicPoly;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Storage interface for polydata records.
 */
public interface Polydata extends Closeable {

    BasicPoly create(String name);

    /**
     * Check if exists poly storage.
     * @param name
     * @return
     */
    boolean exists(String name);

    Optional<BasicPoly> config(String name);

    void config(String name, BasicPoly config);

    Optional<BasicPoly> metadata(String name);

    void metadata(String name, BasicPoly metadata);

    /**
     * Fetch tags from specific index.
     * Examples:
     *  TagIndex: tags
     *  Values: dogs: 10, cats: 2
     *
     *  TagIndex: _date
     *  Values: _count: 1000
     */
    Optional<BasicPoly> fetchTagIndex(String name);

    BasicPolyList persistPoly(String name, Collection<PersistRequest> persistRequests);

    BasicPolyList updatePoly(String name,Collection<PersistRequest> persistRequests);

    BasicPolyList readPoly(String name, Set<String> ids);

    BasicPolyList removePoly(String name, Set<String> ids);

    BasicPolyList query(String name, PolyQuery polyQuery);

    /** Return tag information **/
    BasicPoly fetchPolyTagInfo(String name, String tagId);

    /**
     * List available polys
     * @return
     */
    BasicPolyList listPolys();

    void open();

    void close() throws IOException;

}
