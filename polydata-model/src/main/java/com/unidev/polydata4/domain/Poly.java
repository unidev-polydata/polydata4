package com.unidev.polydata4.domain;

import java.io.Serializable;
import java.util.Map;

/**
 * Poly - storage for polydata records
 * Each poly predefined field for _id
 */
public interface Poly extends Serializable {

    String ID_KEY = "_id";

    /**
     * Fetch Poly ID
     *
     * @return poly id or null
     */
    String _id();

    /**
     * Set poly id
     *
     * @param id New poly id
     * @return poly instance
     */
    <P extends Poly> P _id(String id);

    /**
     * Set value in poly and return poly
     *
     * @return Updated poly
     */
    <T, P extends Poly> P with(String key, T value);

    <T> void put(String key, T value);

    /**
     * Fetch metadata by key, if value is missing, null is returned
     */
    <T> T fetch(String key);

    /**
     * Return default by key, if value is missing, defaultValue is returned
     */
    <T> T fetch(String key, T defaultValue);

    /**
     * Delete poly key.
     */
    <P extends Poly> P delete(String key);

    /**
     * Fetch poly data.
     */
    Map<String, Object> data();

    Map<String, Object> getData();

    void setData(Map<String, Object> data);

    <P extends Poly> P withData(Map<String, Object> data);

    <P extends PolyList> void putPolyList(String key, P polyList);

    <P extends PolyList> P getPolyList(String key);

    <P extends PolyList> P getPolyList(String key, P defaultValue);

    <P extends PolyMap> void putPolyMap(String key, P polyMap);

    <P extends PolyMap> P getPolyMap(String key);

    <P extends PolyMap> P getPolyMap(String key, P defaultValue);

    boolean containsKey(String key);

    Map<String, Object> metadata();

    Map<String, Object> getMetadata();

    void setMetadata(Map<String, Object> metadata);

    <P extends Poly> P withMetadata(Map<String, Object> metadata);

    <T, P extends Poly> P withMetadata(String key, T value);

    void putMetadata(String key, Object value);

    <T> T fetchMetadata(String key);

    <T> T fetchMetadata(String key, T defaultValue);

    <P extends Poly> P deleteMetadata(String key);

}
