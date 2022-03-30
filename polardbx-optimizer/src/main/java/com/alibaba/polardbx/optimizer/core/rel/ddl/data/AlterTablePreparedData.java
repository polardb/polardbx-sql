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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.calcite.sql.SqlEnableKeys;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@EqualsAndHashCode(callSuper = true)
@Data
public class AlterTablePreparedData extends DdlPreparedData {

    /**
     * Column modifications
     */
    private List<String> droppedColumns;
    private List<String> addedColumns;
    private List<String> updatedColumns;
    private Map<String, String> changedColumns;
    private List<String> alterDefaultColumns;

    private boolean timestampColumnDefault;
    Map<String, String> binaryColumnDefaultValues;

    /**
     * Index modifications
     */
    private List<String> droppedIndexes;
    private List<String> addedIndexes;
    private List<String> addedIndexesWithoutNames;
    private Map<String, String> renamedIndexes;

    /**
     * Primary key modifications
     */
    private boolean primaryKeyDropped = false;
    private List<String> addedPrimaryKeyColumns;

    private List<Pair<String, String>> columnAfterAnother;
    private boolean logicalColumnOrder;

    private String tableComment;
    private String tableRowFormat;

    /**
     * Partition modifications
     */
    private Set<String> truncatePartitionNames;

    /**
     * Charset and collation
     */
    private String charset;
    private String collate;

    /**
     * Enable/Disable keys
     * Three status: null, true, false
     */
    private SqlEnableKeys.EnableType enableKeys;

    /**
     * Whether need backfill for add column
     */
    private List<String> backfillColumns;

    public boolean hasColumnModify() {
        return GeneralUtil.isNotEmpty(droppedColumns) ||
            GeneralUtil.isNotEmpty(addedColumns) ||
            GeneralUtil.isNotEmpty(updatedColumns) ||
            GeneralUtil.isNotEmpty(changedColumns);
    }
}
