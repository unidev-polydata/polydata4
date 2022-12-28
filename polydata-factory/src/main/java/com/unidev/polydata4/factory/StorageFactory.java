package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import org.apache.commons.lang3.StringUtils;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

/**
 * Interface for storage factories
 */
public interface StorageFactory {

    /**
     * Fetch cache provider from configuration
     */
    static Optional<Cache<String, BasicPoly>> fetchCache(BasicPoly config) {
        if (config == null) {
            return Optional.empty();
        }
        String cacheType = config.fetch("type", "");

        if (cacheType.equals("jcache")) {
            String cacheProvider = config.fetch("provider", "");
            String cacheName = config.fetch("name", "");
            String implementationUri = config.fetch("implementationUri", "");

            CachingProvider provider = null;
            if (StringUtils.isBlank(cacheProvider)) {
                provider = Caching.getCachingProvider();
            } else {
                provider = Caching.getCachingProvider(cacheProvider);
            }

            Cache<String, BasicPoly> cache = null;
            if (StringUtils.isNotBlank(implementationUri)) {
                try {
                    CacheManager cacheManager = provider.getCacheManager(new URI(implementationUri), null);
                    Optional<MutableConfiguration<String, BasicPoly>> configuration = fetchJCacheConfig(config);
                    if (configuration.isPresent()) {
                        cache = cacheManager.createCache(cacheName, configuration.get());
                    } else {
                        cache = cacheManager.getCache(cacheName);
                    }

                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            } else {
                Optional<MutableConfiguration<String, BasicPoly>> configuration = fetchJCacheConfig(config);
                if (configuration.isPresent()) {
                    cache = provider.getCacheManager().createCache(cacheName, configuration.get());
                } else {
                    cache = provider.getCacheManager().getCache(cacheName);
                }
            }

            return Optional.of(cache);
        }
        return Optional.empty();
    }

    static Optional<MutableConfiguration<String, BasicPoly>> fetchJCacheConfig(BasicPoly config) {
        MutableConfiguration<String, BasicPoly> jcacheConfig = new MutableConfiguration<>();
        //TODO: add jcache configuration
        return Optional.of(jcacheConfig);
    }

    /**
     * Return supported storage type.
     */
    String type();

    /**
     * Create polydata from configuration.
     */
    Optional<Polydata> create(BasicPoly config);


}
