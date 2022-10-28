/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.repository;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ziyang.lb
 **/
public class SchemaObjectStoreInMemory implements SchemaObjectStore {
    private final Map<Long, SchemaObject> objects = new ConcurrentHashMap<Long, SchemaObject>(16, 0.75f, 1);
    private final Map<Long, SchemaObject> indexes = new ConcurrentHashMap<Long, SchemaObject>(16, 0.75f, 1);
    private final Map<Long, SchemaObject> sequences = new ConcurrentHashMap<Long, SchemaObject>(16, 0.75f, 1);
    private final Map<Long, SchemaObject> functions = new ConcurrentHashMap<Long, SchemaObject>(16, 0.75f, 1);
    private Schema schema;

    @Override
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public SchemaObject getObject(Long key) {
        return objects.get(key);
    }

    @Override
    public SchemaObject getIndex(Long key) {
        return indexes.get(key);
    }

    @Override
    public SchemaObject getSequence(Long key) {
        return sequences.get(key);
    }

    @Override
    public SchemaObject getFunction(Long key) {
        return functions.get(key);
    }

    @Override
    public Collection<SchemaObject> getAllObjects() {
        return objects.values();
    }

    @Override
    public Collection<SchemaObject> getAllIndexes() {
        return indexes.values();
    }

    @Override
    public Collection<SchemaObject> getAllSequences() {
        return sequences.values();
    }

    @Override
    public Collection<SchemaObject> getAllFunctions() {
        return functions.values();
    }

    @Override
    public void addObject(Long key, SchemaObject schemaObject) {
        objects.put(key, schemaObject);
    }

    @Override
    public void removeObject(Long key) {
        objects.remove(key);
    }

    @Override
    public void addIndex(Long key, SchemaObject schemaObject) {
        indexes.put(key, schemaObject);
    }

    @Override
    public void removeIndex(Long key) {
        indexes.remove(key);
    }

    @Override
    public void addSequence(Long key, SchemaObject schemaObject) {
        sequences.put(key, schemaObject);
    }

    @Override
    public void removeSequence(Long key) {
        sequences.remove(key);
    }

    @Override
    public void addFunction(Long key, SchemaObject schemaObject) {
        functions.put(key, schemaObject);
    }

    @Override
    public void removeFunction(Long key) {
        functions.remove(key);
    }

    @Override
    public void clearAll() {
        this.objects.clear();
        this.functions.clear();
        this.sequences.clear();
        this.indexes.clear();
    }
}
