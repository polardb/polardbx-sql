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

/**
 * Store for save schema objects
 *
 * @author ziyang.lb
 */
public interface SchemaObjectStore {

    void setSchema(Schema schema);

    void addObject(Long key, SchemaObject schemaObject);

    void removeObject(Long key);

    SchemaObject getObject(Long key);

    Collection<SchemaObject> getAllObjects();

    void addIndex(Long key, SchemaObject schemaObject);

    void removeIndex(Long key);

    SchemaObject getIndex(Long key);

    Collection<SchemaObject> getAllIndexes();

    void addSequence(Long key, SchemaObject schemaObject);

    void removeSequence(Long key);

    SchemaObject getSequence(Long key);

    Collection<SchemaObject> getAllSequences();

    void addFunction(Long key, SchemaObject schemaObject);

    void removeFunction(Long key);

    SchemaObject getFunction(Long key);

    Collection<SchemaObject> getAllFunctions();

    void clearAll();
}
