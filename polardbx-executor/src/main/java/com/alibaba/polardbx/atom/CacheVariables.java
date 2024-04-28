/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.alibaba.polardbx.atom;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class CacheVariables {

    private static final long expireTime = 3600 * 1000;
    private static final long maxCacheSize = 1024;

    /**
     * <pre>
     *     key: the name of DruidDataSource, dbkey + auto_incr_ver
     *     val: the local variables of DruidDataSource
     * </pre>
     */
    protected static Cache<String, Map<String, Object>> cache = newCache();

    /**
     * <pre>
     *     key: the name of DruidDataSource, dbkey + auto_incr_ver
     *     val: the global variables of DruidDataSource
     * </pre>
     */
    protected static Cache<String, Map<String, Object>> globalCache = newCache();

    private static Cache<String, Map<String, Object>> newCache() {
        Cache<String, Map<String, Object>> cache = CacheBuilder.newBuilder()
            .maximumSize(maxCacheSize)
            .expireAfterWrite(expireTime, TimeUnit.MILLISECONDS)
            .softValues()
            .build();
        return cache;
    }


    public static void invalidateAll() {
        cache.invalidateAll();
        globalCache.invalidateAll();
    }

}
