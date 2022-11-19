package com.unidev.polydata4.domain;

import lombok.*;

import java.io.Serializable;

/**
 * Polydata query
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class BasicPolyQuery implements PolyQuery, Serializable {

    public enum QueryType {NEXT, PREV, RANDOM, FETCH_BY_ID, FETCH_BY_INDEX}

    public static final String QUERY_TYPE = "queryType";
    public static final String QUERY_IDS = "queryIds";
    public static final String QUERY_KEY = "queryKey"; // next key / prev key

    public static final String TAG_KEY = "tagKey";
    public static final String TAG_VALUE = "tagValue";

    public static final String COUNT_KEY = "count";

    public static final String PAGE_KEY = "page";

    @Builder.Default
    private BasicPoly options = new BasicPoly();

    public void queryType(QueryType queryType) {
        options.put(QUERY_TYPE, queryType.name());
    }

    public QueryType queryType() {
        return QueryType.valueOf(QueryType.class, options.fetch(QUERY_TYPE, QueryType.NEXT.name()));
    }

    public void queryIds(BasicPolyList basicPolyList) {
        options.putPolyList(QUERY_IDS, basicPolyList);
    }

    public BasicPolyList queryIds() {
        return options.getPolyList(QUERY_IDS);
    }

    public String queryKey() {
        return options.fetch(QUERY_KEY);
    }

    public void queryKey(String key) {
        options.put(QUERY_KEY, key);
    }

    public String tagValue() {
        return options.fetch(TAG_VALUE);
    }

    public void queryTag(String tagValue) {
        options.put(TAG_VALUE, tagValue);
    }

    public Integer count() {
        return Integer.parseInt(options.fetch(COUNT_KEY, "0") + "");
    }

    public void count(int count) {
        options.put(COUNT_KEY, count + "");
    }

    public Integer page() {
        return Integer.parseInt(options.fetch(PAGE_KEY, "0") + "");
    }

    public void page(int page) {
        options.put(PAGE_KEY, page + "");
    }


}
