package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.flatfiles.PolydataSingleJson;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Optional;

@Slf4j
public class FlatFileJsonFactory extends StorageFactory {
    @Override
    public String type() {
        return "flat-file-json";
    }

    @Override
    public Optional<Polydata> create(BasicPoly config) {
        if (!config.containsKey("root")) {
            log.warn("Missing root path");
            return Optional.empty();
        }
        String root = config.fetch("root") + "";
        PolydataSingleJson polydataJson = new PolydataSingleJson(new File(root));
        return Optional.of(polydataJson);
    }

}
