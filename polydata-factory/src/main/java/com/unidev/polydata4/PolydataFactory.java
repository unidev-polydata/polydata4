package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.factory.MongodbStorageFactory;
import com.unidev.polydata4.factory.StorageFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Factory of Polydata storages.
 *
 * Example configuration:
 * {
 *  type: "mongodb"
 *  uri: "mongodb://localhost:27017"
 *  ...
 *  cache: {
 *      type: "jcache"
 *      provider: ""
 *      name: ""
 *  }
 * }
 *
 *
 */
@Slf4j
public class PolydataFactory {

    @Getter
    @Setter
    private Map<String, StorageFactory> storageMap = new HashMap<>();

    public PolydataFactory() {
        // default storage factories
        addFactory(new MongodbStorageFactory());
    }

    public void addFactory(StorageFactory factory) {
        String type = factory.type();
        storageMap.put(type, factory);
    }

    public Optional<Polydata> create(BasicPoly config) {
        if (config == null) {
            log.warn("Empty config");
            return Optional.empty();
        }
        String type = config.fetch("type");
        StorageFactory storageFactory = storageMap.get(type);
        if (storageFactory == null) {
            log.warn("Failed to get factory {} ", type);
            return Optional.empty();
        }
        return storageFactory.create(config);
    }

}
