package com.unidev.polydata4.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.platform.common.DataTransform;
import com.unidev.platform.common.exception.UnidevRuntimeException;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic storage for polydata records
 */
public class BasicPoly implements Poly, Serializable {

    @Getter
    @Setter
    protected Map<String, Object> data;

    @Getter
    @Setter
    protected Map<String, Object> metadata;

    public BasicPoly(Map<String, Object> data, Map<String, Object> metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public BasicPoly() {
        data = new ConcurrentHashMap<>();
        metadata = new ConcurrentHashMap<>();
    }

    public BasicPoly(BasicPoly copyPoly) {
        data = new ConcurrentHashMap<>();
        metadata = new ConcurrentHashMap<>();

        if (copyPoly.data != null) {
            for (Map.Entry<String, Object> entry : copyPoly.data().entrySet()) {
                if (entry.getValue() != null) {
                    data.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (copyPoly.metadata != null) {
            for (Map.Entry<String, Object> entry : copyPoly.metadata().entrySet()) {
                if (entry.getValue() != null) {
                    metadata.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    /**
     * Build new poly instance
     *
     * @return new poly instance
     */
    public static BasicPoly newPoly() {
        return new BasicPoly();
    }

    /**
     * New poly instance with id
     *
     * @param id Poly id
     * @return new poly instance with provided id
     */
    public static BasicPoly newPoly(String id) {
        return new BasicPoly()._id(id);
    }

    public static BasicPoly newPoly(BasicPoly data) {
        return new BasicPoly(data);
    }

    /**
     * Fetch metadata by key, if value is missing, null is returned
     *
     * @param key Metadata key
     * @return value by key or null
     */
    public <T> T fetch(String key) {
        if (data == null) {
            return null;
        }
        if (!data.containsKey(key)) {
            return null;
        }
        return (T) data.get(key);
    }

    /**
     * Return default by key, if value is missing, defaultValue is returned
     *
     * @param key
     * @param defaultValue
     * @param <T>
     * @return
     */
    public <T> T fetch(String key, T defaultValue) {
        if (data == null) {
            return defaultValue;
        }
        if (!data.containsKey(key)) {
            return defaultValue;
        }
        return (T) data.get(key);
    }

    public <T> T fetch(String key, Class<T> clazz) {
        if (data == null) {
            return null;
        }
        if (!data.containsKey(key)) {
            return null;
        }
        DataTransform dataTransform = new DataTransform(new ObjectMapper());
        Optional<T> result = dataTransform.toObject(data.get(key), clazz);
        if (result.isPresent()) {
            return result.get();
        }
        return null;
    }

    public <T> T fetch(String key, Class<T> clazz, T defaultValue) {
        if (data == null) {
            return defaultValue;
        }
        if (!data.containsKey(key)) {
            return defaultValue;
        }
        DataTransform dataTransform = new DataTransform(new ObjectMapper());
        Optional<T> result = dataTransform.toObject(data.get(key), clazz);
        if (result.isPresent()) {
            return result.get();
        }
        return defaultValue;
    }

    @Override
    public <P extends Poly> P delete(String key) {
        data.remove(key);
        return (P) this;
    }

    @Override
    public Map<String, Object> getData() {
        return data;
    }

    @Override
    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public Map<String, Object> data() {
        return data;
    }

    @Override
    public <P extends Poly> P withData(Map<String, Object> data) {
        this.data = data;
        return (P) this;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return this.metadata;
    }

    @Override
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Override
    public <P extends Poly> P withMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return (P) this;
    }

    @Override
    public <T, P extends Poly> P withMetadata(String key, T value) {
        metadata.put(key, value);
        return (P) this;
    }

    @Override
    public void putMetadata(String key, Object value) {
        metadata.put(key, value);
    }

    @Override
    public <T> T fetchMetadata(String key) {
        return fetchMetadata(key, null);
    }

    @Override
    public <T> T fetchMetadata(String key, T defaultValue) {
        if (metadata == null) {
            return defaultValue;
        }
        if (!metadata.containsKey(key)) {
            return defaultValue;
        }
        return (T) metadata.get(key);
    }

    @Override
    public <P extends Poly> P deleteMetadata(String key) {
        metadata.remove(key);
        return (P) this;
    }

    public String _id() {
        return fetch(ID_KEY) + "";
    }

    public BasicPoly _id(String id) {
        put(ID_KEY, id);
        return this;
    }

    @Override
    public <T, P extends Poly> P with(String key, T value) {
        put(key, value);
        return (P) this;
    }

    public String get_id() {
        return fetch(ID_KEY);
    }

    public void set_id(String _id) {
        put(ID_KEY, _id);
    }

    @Override
    public boolean containsKey(String key) {
        if (data == null) {
            return false;
        }
        return data.containsKey(key);
    }

    @Override
    public <P extends PolyList> void putPolyList(String key, P polyList) {
        put(key, polyList);
    }

    @Override
    public <P extends PolyList> P getPolyList(String key) {
        return getPolyList(key, null);
    }

    @Override
    public <P extends PolyList> P getPolyList(String key, P defaultValue) {
        Object item = fetch(key);
        if (item == null || data == null) {
            return defaultValue;
        }
        if (item instanceof PolyList) {
            return (P) item;
        }
        if (item instanceof LinkedHashMap) {
            //TODO: replace parsing
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return (P) objectMapper.readValue(objectMapper.writeValueAsString(item), BasicPolyList.class);
            } catch (JsonProcessingException e) {
                throw new UnidevRuntimeException(e);
            }
        }
        return (P) item;
    }

    @Override
    public <P extends PolyMap> void putPolyMap(String key, P polyList) {
        put(key, polyList);
    }

    @Override
    public <P extends PolyMap> P getPolyMap(String key) {
        return getPolyMap(key, null);
    }

    @Override
    public <P extends PolyMap> P getPolyMap(String key, P defaultValue) {
        Object item = fetch(key);
        if (item == null || data == null) {
            return defaultValue;
        }
        if (item instanceof PolyList) {
            return (P) item;
        }
        if (item instanceof LinkedHashMap) {
            //TODO: replace parsing
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return (P) objectMapper.readValue(objectMapper.writeValueAsString(item), BasicPolyMap.class);
            } catch (JsonProcessingException e) {
                throw new UnidevRuntimeException(e);
            }
        }
        return (P) item;
    }

    @Override
    public <T> void put(String key, T value) {
        data.put(key, value);
    }

    @Override
    public String toString() {
        return "BasicPoly{" +
                "data=" + data +
                ", metadata=" + metadata +
                '}';
    }
}
