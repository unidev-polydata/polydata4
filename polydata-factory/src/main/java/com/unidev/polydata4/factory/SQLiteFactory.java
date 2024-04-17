package com.unidev.polydata4.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.sqlite.PolydataSqlite;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Optional;

@Slf4j
public class SQLiteFactory extends StorageFactory {
    @Override
    public String type() {
        return "sqlite";
    }

    @Override
    public Optional<Polydata> create(BasicPoly config) {
        if (!config.containsKey("root")) {
            log.warn("Missing root path");
            return Optional.empty();
        }
        String root = config.fetch("root") + "";
        return Optional.of(new PolydataSqlite(new File(root), new ObjectMapper()));
    }
}
