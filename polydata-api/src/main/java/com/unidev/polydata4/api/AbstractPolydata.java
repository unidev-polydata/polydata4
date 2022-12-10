package com.unidev.polydata4.api;

import com.unidev.polydata4.domain.BasicPoly;

import javax.cache.Cache;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Common polydata handling logic
 *
 */
public abstract class AbstractPolydata implements Polydata {

    protected Optional<Cache<String, BasicPoly>> cache;

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

}
