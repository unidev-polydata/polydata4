package com.unidev.polydata4.flatfiles;

import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import lombok.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.unidev.polydata4.api.Polydata.INDEXES;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class FlatFileRepository {

    public static final String TIMESTAMP_KEY = "_timestamp";

    private String poly;
    private BasicPoly metadata;

    private BasicPoly config;

    @Getter
    private Map<String, BasicPoly> polyById = new ConcurrentHashMap<>();

    @Getter
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

    /**
     * Return all polys from index.
     *
     * @param index
     * @return
     */
    public BasicPolyList fetchPolysFromIndex(String index) {
        BasicPolyList list = new BasicPolyList();
        List<String> polysById = polyIndex.get(index);
        if (polysById == null) {
            return list;
        }
        polysById.forEach(polyId -> {
            BasicPoly poly = polyById.get(polyId);
            if (poly != null) {
                list.add(poly);
            }
        });
        return list;
    }

    /**
     * Add poly to index
     */
    public void add(BasicPoly basicPoly, Collection<String> indexes) {
        basicPoly.put(INDEXES, indexes);
        if (!basicPoly.containsKey(TIMESTAMP_KEY)) {
            basicPoly.put(TIMESTAMP_KEY, System.nanoTime());
        }
        polyById.put(basicPoly._id(), basicPoly);
        for (String index : indexes) {
            polyIndex.putIfAbsent(index, new ArrayList<>());
            List<String> list = polyIndex.get(index);
            if (list.contains(basicPoly._id())) {
                continue;
            }
            list.add(basicPoly._id());
            list.sort((o1, o2) -> {
                Number t1 = polyById.get(o1).fetch(TIMESTAMP_KEY, 0L);
                Number t2 = polyById.get(o2).fetch(TIMESTAMP_KEY, 0L);
                return Long.compare(t2.longValue(), t1.longValue());
            });
        }
    }

    /**
     * Remove poly from index
     */
    public void remove(String id) {
        BasicPoly basicPoly = polyById.get(id);
        if (basicPoly == null) {
            return;
        }
        Collection<String> indexes = basicPoly.fetch(INDEXES);
        for (String index : indexes) {
            List<String> list = polyIndex.get(index);
            if (list == null) {
                continue;
            }
            list.remove(id);
        }
        polyById.remove(id);
    }

}
