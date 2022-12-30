package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.mongodb.PolydataMongodb;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class MongodbFactory implements StorageFactory {

    @Override
    public String type() {
        return "mongodb";
    }

    @Override
    public Optional<Polydata> create(BasicPoly config) {
        if (!config.containsKey("uri")) {
            log.warn("Missing mongodb URI");
            return Optional.empty();
        }
        String uri = config.fetch("uri") + "";

        PolydataMongodb polydataMongodb = new PolydataMongodb(uri);
        StorageFactory.fetchCache(config.fetch("cache")).ifPresent(polydataMongodb::setCache);
        return Optional.of(polydataMongodb);
    }

}
