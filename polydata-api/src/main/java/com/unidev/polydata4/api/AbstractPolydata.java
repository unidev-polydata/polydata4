package com.unidev.polydata4.api;

import com.unidev.polydata4.domain.BasicPoly;

import javax.cache.Cache;
import java.util.Optional;
import java.util.function.Function;

/**
 * Common polydata handling logic
 *
 */
public abstract class AbstractPolydata implements Polydata {

    protected Optional<Cache<String, BasicPoly>> cache = Optional.empty();

    @Override
    public void setCache(Cache<String, BasicPoly> cache) {
        this.cache = Optional.ofNullable(cache);
    }

    /**
     * Execute logic if cache is present
     */
    protected <R> R ifCache(Function<Cache<String, BasicPoly>, R> logic) {
        return cache.map(logic::apply).orElse(null);
    }

    protected void putIfCache(String key, BasicPoly poly) {
        cache.ifPresent(entries -> entries.put(key, poly));
    }

}
