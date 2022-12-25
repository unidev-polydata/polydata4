package com.unidev.polydata4.flatfiles.yaml;

import com.unidev.polydata4.domain.BasicPoly;
import lombok.*;

import java.io.File;
import java.util.List;
import java.util.Map;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class YamlRepository {
    @Getter
    @Setter
    private String poly;

    @Getter
    @Setter
    private File root;

    // Mapping of id = poly
    @Getter
    @Setter
    private Map<String, BasicPoly> polyMap;

    @Getter
    @Setter
    private Map<String, Integer> indexes;

    // index-name : <list ordered by date>
    @Getter
    @Setter
    private Map<String, List<String>> polyIndexes;

    @Getter
    @Setter
    private BasicPoly metadata;

    @Getter
    @Setter
    private BasicPoly config;

    @Getter
    @Setter
    private Map<String, String> tagLabels;
}
