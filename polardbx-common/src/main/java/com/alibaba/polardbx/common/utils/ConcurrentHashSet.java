package com.alibaba.polardbx.common.utils;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashSet<E> extends AbstractSet<E> implements Set<E> {
    private final Map<E, Boolean> _map = new ConcurrentHashMap();
    private transient Set<E> _keys;

    public ConcurrentHashSet() {
        this._keys = this._map.keySet();
    }

    public boolean add(E e) {
        return this._map.put(e, Boolean.TRUE) == null;
    }

    public void clear() {
        this._map.clear();
    }

    public boolean contains(Object o) {
        return this._map.containsKey(o);
    }

    public boolean containsAll(Collection<?> c) {
        return this._keys.containsAll(c);
    }

    public boolean equals(Object o) {
        return o == this || this._keys.equals(o);
    }

    public int hashCode() {
        return this._keys.hashCode();
    }

    public boolean isEmpty() {
        return this._map.isEmpty();
    }

    public Iterator<E> iterator() {
        return this._keys.iterator();
    }

    public boolean remove(Object o) {
        return this._map.remove(o) != null;
    }

    public boolean removeAll(Collection<?> c) {
        return this._keys.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return this._keys.retainAll(c);
    }

    public int size() {
        return this._map.size();
    }

    public Object[] toArray() {
        return this._keys.toArray();
    }

    public <T> T[] toArray(T[] a) {
        return this._keys.toArray(a);
    }

    public String toString() {
        return this._keys.toString();
    }
}