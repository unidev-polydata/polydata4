package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Representation of raw yaml file.
 */

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class FlatFile extends HashMap<String, Object> {

    private YamlFileMetadata metadata;

    public YamlFileMetadata metadata() {
        return getMetadata();
    }

    public BasicPoly toPoly() {
        BasicPoly poly = BasicPoly.newPoly().withData(this);

        if (metadata != null) {
            poly.setMetadata(metadata);
            poly.getMetadata().put("_index", metadata.getIndex());
        }

        return poly;
    }

    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class YamlFileMetadata extends HashMap<String, Object> {

        private List<String> index;

    }

    public static class StringList extends ArrayList<String> {

    }


}
