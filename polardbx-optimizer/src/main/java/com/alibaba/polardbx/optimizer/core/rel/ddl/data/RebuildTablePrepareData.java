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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
@Data
public class RebuildTablePrepareData {

    /**
     * oldName --> newName
     */
    Map<String, String> tableNameMap = new HashMap<>();

    /**
     * newName --> oldName
     */
    Map<String, String> tableNameMapReverse = new HashMap<>();

    /**
     * for checker
     */
    Map<String, String> srcVirtualColumnMap = new HashMap<>();

    /**
     * for checker
     */
    Map<String, SQLColumnDefinition> srcColumnNewDef = new HashMap<>();

    Map<String, SQLColumnDefinition> dstColumnNewDef = new HashMap<>();

    /**
     * for checker
     */
    Map<String, String> dstVirtualColumnMap = new HashMap<>();

    /**
     * for backfill when change column name
     */
    Map<String, String> backfillColumnMap = new HashMap<>();

    /**
     * use changeset, new gsi name --> false/true
     */
    Map<String, Boolean> needReHash = new HashMap<>();

    /**
     * modify the string type columns ,which should not use select binary, set bytes
     */
    List<String> modifyStringColumns = new ArrayList<>();

    /**
     * for alter table add column
     */
    List<String> addNewColumns = new ArrayList<>();

    /**
     * for alter table drop column
     */
    List<String> dropColumns = new ArrayList<>();

    /**
     * for alter table modify column
     */
    List<String> modifyColumns = new ArrayList<>();

    /**
     * for prefix index, the length of new column def
     */
    Map<String, Long> columnLengthMap = new HashMap<>();

    /**
     * for cci
     */
    long versionId;
}
