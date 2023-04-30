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

    String COUNT = "count";

    String RANDOM_COUNT = "random_count";


    void prepareStorage();

    BasicPoly create(String dataset);

    /**
     * Check if exists poly storage.
     *
     * @param dataset
     * @return
     */
    boolean exists(String dataset);

    Optional<BasicPoly> config(String dataset);

    void config(String dataset, BasicPoly config);

    Optional<BasicPoly> metadata(String dataset);

    void metadata(String dataset, BasicPoly metadata);

    /**
     * Fetch tags from specific index.
     * Examples:
     * index("cats") = "{count: 10}"
     */
    Optional<BasicPoly> index(String dataset);

    /**
     * Return index information
     **/
    Optional<BasicPoly> indexData(String dataset, String indexId);

    BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests);

    BasicPolyList update(String dataset, Collection<InsertRequest> insertRequests);

    BasicPolyList read(String dataset, Set<String> ids);

    BasicPolyList remove(String dataset, Set<String> ids);

    BasicPolyList query(String dataset, PolyQuery polyQuery);

    Long count(String dataset, PolyQuery polyQuery);

    /**
     * List available polys
     */
    BasicPolyList list();

    void open();

    void close() throws IOException;

    void setCache(Cache<String, BasicPoly> cache);

}
