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

    public static final String QUERY_FUNCTION = "queryFunction";
    public static final String QUERY_IDS = "ids";
    public static final String INDEX = "index";
    public static final String PAGE = "page";
    @Builder.Default
    private BasicPoly options = new BasicPoly();

    public void queryIds(BasicPolyList basicPolyList) {
        options.putPolyList(QUERY_IDS, basicPolyList);
    }

    public void queryType(QueryFunction queryType) {
        options.put(QUERY_FUNCTION, queryType.name());
    }

    public QueryFunction queryType() {
        return QueryFunction.valueOf(QueryFunction.class, options.fetch(QUERY_FUNCTION, QueryFunction.PAGES.name()));
    }

    public BasicPolyList queryIds() {
        return options.getPolyList(QUERY_IDS);
    }

    public String index() {
        return options.fetch(INDEX);
    }

    public void index(String index) {
        options.put(INDEX, index);
    }

    public Integer page() {
        return Integer.parseInt(options.fetch(PAGE, "0") + "");
    }

    public void page(int page) {
        options.put(PAGE, page + "");
    }

    public <T> T option(String key) {
        return options.fetch(key);
    }

    public <T> T option(String key, T value) {
        return options.fetch(key, value);
    }

    public <T> void withOption(String key, T value) {
        options.put(key, value);
    }

    public enum QueryFunction {PAGES, RANDOM, SEARCH}


}
