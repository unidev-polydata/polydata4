package com.unidev.polydata4.factory;

import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface for storage factories
 */
@Slf4j
public abstract class StorageFactory {

    static Optional<MutableConfiguration<String, BasicPoly>> fetchJCacheConfig(BasicPoly config) {
        MutableConfiguration<String, BasicPoly> jcacheConfig = new MutableConfiguration<>();
        //TODO: add jcache configuration
        return Optional.of(jcacheConfig);
    }

    protected Map<String, CachingProvider> cachingProviders = new HashMap<>();
    /**
     * Fetch cache provider from configuration
     */
    public Optional<Cache<String, BasicPoly>> fetchCache(BasicPoly config) {
        if (config == null) {
            return Optional.empty();
        }
        String cacheType = config.fetch("type", "");
        log.info("Fetching cache for type {}", cacheType);
        if (cacheType.equals("jcache")) {
            String cacheProvider = config.fetch("provider", "");
            String cacheName = config.fetch("name", "");
            String implementationUri = config.fetch("implementationUri", "");

            CachingProvider provider = null;
            if (StringUtils.isBlank(cacheProvider)) {
                provider = Caching.getCachingProvider();
            } else {
                provider = cachingProviders.computeIfAbsent(cacheProvider, key -> {
                    try {
                        return Caching.getCachingProvider(cacheProvider);
                    } catch (Exception e) {
                        log.error("Failed to get caching provider {}", cacheProvider, e);
                        return null;
                    }
                });
                if (provider == null) {
                    log.error("Failed to get caching provider {}", cacheProvider);
                    return Optional.empty();
                }
            }

            Cache<String, BasicPoly> cache = null;
            if (StringUtils.isNotBlank(implementationUri)) {
                try {
                    CacheManager cacheManager = provider.getCacheManager(new URI(implementationUri), null);
                    AtomicBoolean cacheExists = new AtomicBoolean(false);
                    cacheManager.getCacheNames().iterator().forEachRemaining(name -> {
                        if (name.equals(cacheName)) {
                            cacheExists.set(true);
                        }
                    });

                    if (!cacheExists.get()) {
                        Optional<MutableConfiguration<String, BasicPoly>> configuration = fetchJCacheConfig(config);
                        if (configuration.isPresent()) {
                            cache = cacheManager.createCache(cacheName, configuration.get());
                        } else {
                            cache = cacheManager.getCache(cacheName);
                        }
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

    /**
     * Return supported storage type.
     */
    public abstract String type();

    /**
     * Create polydata from configuration.
     */
    public abstract Optional<Polydata> create(BasicPoly config);


}
