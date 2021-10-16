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

package com.alibaba.polardbx.optimizer.config.schema;

import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import org.apache.calcite.schema.Table;

/**
 * @author lingce.ldm 2017-07-06 17:54
 */
public class TddlSchema extends AbsSchema {

    private final SchemaManager schemaManager;
    private final String schemaName;

    public TddlSchema(String schemaName, SchemaManager schemaManager) {
        this.schemaName = schemaName;
        this.schemaManager = schemaManager;
    }

    @Override
    public Table getTable(String tableName) {
        return this.schemaManager.getTable(tableName);
    }
}
