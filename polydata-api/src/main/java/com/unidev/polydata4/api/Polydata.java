package com.unidev.polydata4.api;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.InsertRequest;
import com.unidev.polydata4.domain.PolyQuery;

import javax.cache.Cache;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Storage interface for polydata records.
 */
public interface Polydata extends Closeable {

    String POLY = "poly";
    String _ID = "_id";

    String ITEM_PER_PAGE = "item_per_page";

    int DEFAULT_ITEM_PER_PAGE = 10;

    String CONFIG_KEY = "config";
    String METADATA_KEY = "metadata";

    String DATE_INDEX = "_date";

    String INDEXES = "_indexes";


    void prepareStorage();

    BasicPoly create(String poly);

    /**
     * Check if exists poly storage.
     *
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
     * index("cats") = "{count: 10}"
     */
    Optional<BasicPoly> index(String poly);

    /**
     * Return index information
     **/
    Optional<BasicPoly> indexData(String poly, String indexId);

    BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests);

    BasicPolyList update(String poly, Collection<InsertRequest> insertRequests);

    BasicPolyList read(String poly, Set<String> ids);

    BasicPolyList remove(String poly, Set<String> ids);

    BasicPolyList query(String poly, PolyQuery polyQuery);

    Long count(String poly, PolyQuery polyQuery);

    /**
     * List available polys
     */
    BasicPolyList list();

    void open();

    void close() throws IOException;

    void setCache(Cache<String, BasicPoly> cache);

}
