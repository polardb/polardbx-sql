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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.calcite.sql.SqlEnableKeys;

import java.util.List;
import java.util.Map;
import java.util.Set;

@EqualsAndHashCode(callSuper = true)
@Data
public class AlterTablePreparedData extends DdlPreparedData {

    /**
     * Column modifications
     */
    private List<String> droppedColumns;
    private List<String> addedColumns;
    private List<String> updatedColumns;
    private List<Pair<String, String>> changedColumns;
    private List<String> alterDefaultColumns;
    private List<String> alterDefaultNewColumns;

    private boolean timestampColumnDefault;
    private Map<String, String> specialDefaultValues;
    private Map<String, Long> specialDefaultValueFlags;

    /**
     * Index modifications
     */
    private List<String> droppedIndexes;
    private List<String> addedIndexes;
    private List<String> addedIndexesWithoutNames;
    private List<Pair<String, String>> renamedIndexes;

    /**
     * Foreign key
     */
    private List<String> referencedTables; // Set this if push down physical foreign key.
    private List<ForeignKeyData> addedForeignKeys;
    private List<String> droppedForeignKeys;

    /**
     * Primary key modifications
     */
    private boolean primaryKeyDropped = false;
    private List<String> addedPrimaryKeyColumns;

    private List<Pair<String, String>> columnAfterAnother;

    //Pair<indexName, visibility>
    private List<Pair<String, String>> indexVisibility;
    private boolean logicalColumnOrder;

    private String tableComment;
    private String tableRowFormat;

    /**
     * Partition modifications
     */
    private Set<String> truncatePartitionNames;

    /**
     * Files modifications
     */
    private List<String> dropFiles;

    private String timestamp;

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

    private Boolean isGsi = false;

    private boolean needRepartition = false;

    private boolean keepPartitionKeyRange = true;

    private boolean needDropImplicitKey = false;

    private TableMeta newTableMeta;

    public boolean hasColumnModify() {
        return GeneralUtil.isNotEmpty(droppedColumns) ||
            GeneralUtil.isNotEmpty(addedColumns) ||
            GeneralUtil.isNotEmpty(updatedColumns) ||
            GeneralUtil.isNotEmpty(changedColumns);
    }

    /**
     * Used by online modify column
     */

    private boolean onlineModifyColumn = false;
    private boolean onlineChangeColumn = false;

    private String modifyColumnType = null;
    private String modifyColumnTypeNullable = null;
    private String modifyColumnName = null;

    private String tmpColumnName = null;

    // [tableName -> [localIndexName -> newLocalIndexName]]
    private Map<String, Map<String, String>> localIndexNewNameMap;
    // [tableName -> [localIndexName -> tmpLocalIndexName]]
    private Map<String, Map<String, String>> localIndexTmpNameMap;
    // [tableName -> [localIndexName -> indexMeta]]
    private Map<String, Map<String, IndexMeta>> localIndexMeta;

    // new unique index name if column definition contains unique constraint
    private Map<String, String> newUniqueIndexNameMap;

    private boolean oldColumnNullable;
    private boolean newColumnNullable;

    // if algorithm=omc_index
    private boolean onlineModifyColumnIndexTask = false;

    /**
     * Used by online modify column / add generated column
     */
    private boolean useChecker;
    private boolean useSimpleChecker;
    private List<String> checkerColumnNames;

    private boolean skipBackfill = false;
    private boolean forceCnEval = false;

    private Map<String, Pair<String, String>> notNullableGeneratedColumnDefs;
}
