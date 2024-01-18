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

package com.alibaba.polardbx.common.utils;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Merge a delta map into a base map.
 * <p>
 * Deletes are not allowed in this implementation, instead we recommend to put a null value.
 */
public final class MergeHashMap<K, V> implements Map<K, V> {

    private final HashMap<K, V> delta = new HashMap<>();

    private final Map<K, V> base;

    public MergeHashMap(Map<K, V> base) {
        this.base = base;
    }

    private MergeHashMap(Map<K, V> base, HashMap<K, V> delta) {
        this.base = base;
        this.delta.putAll(delta);
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean isEmpty() {
        return delta.isEmpty() && base.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delta.containsKey(key) || base.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delta.containsValue(value) || base.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return delta.containsKey(key) ? delta.get(key) : base.get(key);
    }

    @Override
    public V put(K key, V value) {
        if (delta.containsKey(key)) {
            return delta.put(key, value);
        } else {
            delta.put(key, value);
            return base.get(key);
        }
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("deletes not supported");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        delta.putAll(m);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("deletes not supported");
    }

    @Override
    public Set<K> keySet() {
        return Sets.union(delta.keySet(), base.keySet());
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("values not supported");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
//        Set entries = Sets.union(delta.entrySet(), base.entrySet());

        /**
         * Use the delta map the newest val of entryset of MergeHashMap, instead of using union which will
         * build a wrong result
         */
        HashMap mergedRsMap = new HashMap(base);
        mergedRsMap.putAll(delta);
        Set entries = mergedRsMap.entrySet();

        return entries;
    }

    public Map<K, V> deepCopy() {
        MergeHashMap newMap = new MergeHashMap(base, delta);
        return newMap;
    }
}
