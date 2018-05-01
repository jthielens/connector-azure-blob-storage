package com.cleo.labs.connector.blobstorage;

import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class AttrCache {

    /**
     * maximumSize=10000,expireAfterWrite=5s
     */
    public static final String DEFAULT_SPEC = "maximumSize=10000,expireAfterWrite=30s";

    private static String spec = null;

    private static Cache<String,Optional<BasicFileAttributeView>> cache = update(DEFAULT_SPEC);

    public static synchronized Cache<String,Optional<BasicFileAttributeView>> update(String newspec) {
        // if the value in the config file is null or missing or blank, this means "default"
        // if the value in the config file is "disabled", this means null
        if (Strings.isNullOrEmpty(newspec)) {
            newspec = DEFAULT_SPEC;
        } else if (newspec.equalsIgnoreCase("disabled")) {
            newspec = null;
        }
        // make adjustments
        if ((spec==null) == (newspec==null) &&
            (spec==null || spec.equals(newspec))) {
            // leave it
        } else if (newspec==null) {
            spec = null ;
            cache = null;
            // logger.debug("Amazon metadata cache disabled");
        } else {
            Cache<String,Optional<BasicFileAttributeView>> newcache = CacheBuilder.from(newspec)
                .build();
            if (cache!=null) {
                newcache.putAll(cache.asMap());
            }
            spec = newspec;
            cache = newcache;
            // logger.debug("Amazon metadata cache established: "+spec);
        }
        return cache;
    }

    private static String key(String clientkey, Path path) {
        return clientkey + "[" + path.toString() + "]";
    }

    public static Optional<BasicFileAttributeView> get(String clientkey, Path path,
            Callable<Optional<BasicFileAttributeView>> getter) throws Exception {
        if (cache!=null) {
            return cache.get(key(clientkey, path), getter);
        } else {
            return getter.call();
        }
    }

    public static void put(String clientkey, Path path, BasicFileAttributeView attr) {
        if (cache!=null) {
            cache.put(key(clientkey, path), Optional.of(attr));
        }
    }

    public static void invalidate(String clientkey, Path path) {
        if (cache!=null) {
            cache.invalidate(key(clientkey, path));
        }
    }

    private AttrCache() {
    }

}