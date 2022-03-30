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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import org.apache.calcite.rel.ddl.RenameTable;
import org.apache.calcite.sql.SqlIdentifier;

public class LogicalRenameTable extends BaseDdlOperation {

    private RenameTablePreparedData renameTablePreparedData;

    public LogicalRenameTable(RenameTable renameTable) {
        super(renameTable);
    }

    public static LogicalRenameTable create(RenameTable renameTable) {
        return new LogicalRenameTable(renameTable);
    }

    public RenameTablePreparedData getRenameTablePreparedData() {
        return renameTablePreparedData;
    }

    public void prepareData() {
        renameTablePreparedData = preparePrimaryData();
    }

    private RenameTablePreparedData preparePrimaryData() {
        RenameTablePreparedData preparedData = new RenameTablePreparedData();

        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(tableName);
        SqlIdentifier newTableName = (SqlIdentifier) relDdl.getNewTableName();
        if (newTableName != null && !newTableName.isSimple()) {
            String targetSchema = newTableName.names.get(0);
            if (OptimizerContext.getContext(targetSchema) == null) {
                throw new TddlNestableRuntimeException("Unknown target database " + targetSchema);
            } else if (!TStringUtil.equalsIgnoreCase(targetSchema, schemaName)) {
                throw new TddlNestableRuntimeException("Target database must be the same as source database");
            }
        }

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setNewTableName(newTableName.getLastName());
        preparedData.setTableVersion(tableMeta.getVersion());

        return preparedData;
    }

}
