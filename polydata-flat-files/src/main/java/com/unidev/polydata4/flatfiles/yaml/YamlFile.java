package com.unidev.polydata4.flatfiles.yaml;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.unidev.polydata4.domain.BasicPoly;
import lombok.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Representation of raw yaml file.
 */

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class YamlFile extends HashMap<String, Object> {

    @JsonProperty("_metadata")
    @Getter
    @Setter
    private YamlFileMetadata metadata;

    @JsonCreator
    public YamlFile(Map<String, Object> map) {
        metadata = PolydataYaml.MAPPER.convertValue(map.get("_metadata"), YamlFileMetadata.class);
        map.remove("_metadata");
        this.putAll(map);
    }

    public YamlFileMetadata metadata() {
        return getMetadata();
    }

    public BasicPoly toPoly() {
        return BasicPoly.newPoly().withData(this).withMetadata(metadata);
    }

    @Data
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class YamlFileMetadata extends HashMap<String, Object> {

        @JsonProperty("_index")
        private List<String> index;

        @JsonCreator
        public YamlFileMetadata(Map<String, Object> map) {
            index = PolydataYaml.MAPPER.convertValue(map.get("_index"), StringList.class);
            this.putAll(map);
        }

    }

    public static class StringList extends ArrayList<String> {

    }


}
