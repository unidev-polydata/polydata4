package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class FlatFileRepository {

    private String poly;
    private BasicPoly metadata;

    private BasicPoly config;

    private Map<String, BasicPoly> polyById = new ConcurrentHashMap<>();

    private Map<String, List<String>> polyIndex = new ConcurrentHashMap<>();

    /**
     * Fetch polys by _id
     */
    public BasicPolyList fetchById(Set<String> ids) {
        BasicPolyList list = new BasicPolyList();
        ids.forEach(id -> {
            BasicPoly poly = polyById.get(id);
            if (poly != null) {
                list.add(poly);
            }
        });
        return list;
    }

    /**
     * Fetch polys from index by order
     */
    public BasicPolyList fetchIndexById(String index, List<Integer> ids) {
        BasicPolyList list = new BasicPolyList();
        List<String> polysById = polyIndex.get(index);
        if (polysById == null) {
            return list;
        }
        ids.forEach(id -> {
            if (id >= polysById.size()) {
                return;
            }
            String polyId = polysById.get(id);
            BasicPoly poly = polyById.get(polyId);
            if (poly != null) {
                list.add(poly);
            }
        });
        return list;
    }

    public void add(BasicPoly basicPoly, List<String> indexes) {
        polyById.put(basicPoly._id(), basicPoly);
        for (String index : indexes) {
            polyIndex.putIfAbsent(index, new ArrayList<>());
            polyIndex.get(index).add(basicPoly._id());
        }
    }

}
