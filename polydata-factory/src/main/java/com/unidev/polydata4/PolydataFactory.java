package com.unidev.polydata4;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.factory.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Factory of Polydata storages.
 * <p>
 * Example configuration:
 * {
 * type: "mongodb"
 * uri: "mongodb://localhost:27017"
 * ...
 * cache: {
 * type: "jcache"
 * provider: ""
 * name: ""
 * }
 * }
 */
@Slf4j
public class PolydataFactory {

    @Getter
    @Setter
    private Map<String, StorageFactory> storageMap = new HashMap<>();

    public PolydataFactory() {
        // default storage factories
        addFactory(new MongodbFactory());
        addFactory(new FlatFileYamlFactory());
        addFactory(new RedisFactory());
        addFactory(new SQLiteFactory());
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
        Optional<Polydata> polydata = storageFactory.create(config);
        polydata.ifPresent(Polydata::prepareStorage);
        return polydata;
    }

}
