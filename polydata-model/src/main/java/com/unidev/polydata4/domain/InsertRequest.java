package com.unidev.polydata4.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Request to persist poly in storage
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InsertRequest implements Serializable {

    private BasicPoly data;
    private Set<String> indexToPersist;

    // additional data to pass for indexing
    private Map<String, BasicPoly> indexData;

}
