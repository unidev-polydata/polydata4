package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import org.apache.commons.lang3.StringUtils;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.util.Optional;

/**
 * Interface for storage factories
 */
public interface StorageFactory {

    /**
     * Return supported storage type.
     */
    String type();

    /**
     * Create polydata from configuration.
     */
    Optional<Polydata> create(BasicPoly config);


    /**
     * Fetch cache provider from configuration
     */
    static Optional<Cache<String, BasicPoly>> fetchCache(BasicPoly config) {
        String cacheType = config.fetch("type", "");

        switch (cacheType) {
            case "jcache":
                String cacheProvider = config.fetch("provider", "");
                String cacheName = config.fetch("name", "");

                CachingProvider provider = null;
                if (StringUtils.isBlank(cacheProvider)) {
                    provider = Caching.getCachingProvider();
                } else {
                    provider = Caching.getCachingProvider(cacheProvider);
                }
                return Optional.of(provider.getCacheManager().getCache(cacheName));
            default:
                return Optional.empty();
        }
    }

}
