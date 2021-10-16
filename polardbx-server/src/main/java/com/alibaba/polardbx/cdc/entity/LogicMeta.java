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

package com.alibaba.polardbx.cdc.entity;

import lombok.Data;

import java.util.List;

/**
 * @author ziyang.lu 2020-12-17
 */
@Data
public class LogicMeta {

    private List<LogicDbMeta> logicDbMetas;

    @Data
    public static class PhySchema {
        private String storageInstId;
        private String group;
        private String schema;
        private List<String> phyTables;
    }

    @Data
    public static class LogicDbMeta {
        private String schema;
        private String charset;
        private List<PhySchema> phySchemas;
        private List<LogicTableMeta> logicTableMetas;
    }

    @Data
    public static class LogicTableMeta {
        private String tableName;
        private int tableMode;
        private int tableType;
        private String createSql;
        private String tableCollation;
        private List<PhySchema> phySchemas;
    }
}
