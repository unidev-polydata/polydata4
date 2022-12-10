package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class MongodbStorageFactory implements StorageFactory {

    @Override
    public String type() {
        return "mongodb";
    }

    @Override
    public Optional<Polydata> create(BasicPoly config) {
        return Optional.empty();
    }

}
