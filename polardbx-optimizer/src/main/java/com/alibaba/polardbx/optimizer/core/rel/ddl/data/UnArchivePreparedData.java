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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Data;

import java.util.Map;

/**
 * @author Shi Yuxuan
 */
@Data
public class UnArchivePreparedData extends DdlPreparedData {
    private String schema;
    private Map<String, Long> tables;
    private String tableGroup;

    public UnArchivePreparedData(String schema, Map<String, Long> tables, String tableGroup) {
        this.schema = schema;
        this.tables = tables;
        this.tableGroup = tableGroup;
    }

    @Override
    public String getSchemaName() {
        return schema;
    }

    public Map<String, Long> getTables() {
        return tables;
    }

    public String getTableGroup() {
        return tableGroup;
    }
}