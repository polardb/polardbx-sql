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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlShowTableReplicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
public class LogicalShowTableReplicateHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowTableReplicateHandler.class);

    public LogicalShowTableReplicateHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTableReplicate showTableReplicate = (SqlShowTableReplicate) show.getNativeSqlNode();

        String schemaName = showTableReplicate.getSchema();
        if (schemaName == null || schemaName.equalsIgnoreCase("information_schema")) {
            return getReplicateResultCursor();
        }

        return handleReplicateResult(executionContext, schemaName);
    }

    private ArrayResultCursor getReplicateResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("TABLE_REPLICATE_STATUS");
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("GSI_NAME", DataTypes.StringType);
        result.addColumn("REPLICATE_STATUS", DataTypes.StringType);
        result.addColumn("GSI_STATUS", DataTypes.StringType);
        result.addColumn("GSI_VISIBILITY", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private ArrayResultCursor handleReplicateResult(ExecutionContext executionContext, String schemaName) {
        ArrayResultCursor result = getReplicateResultCursor();

        Map<String, TableMeta> tableMetaMap = executionContext.getSchemaManager(schemaName).getCache();

        for (Map.Entry<String, TableMeta> entry : tableMetaMap.entrySet()) {
            String tableName = entry.getKey();
            TableMeta tableMeta = entry.getValue();

            if (tableName.equalsIgnoreCase("dual")) {
                continue;
            }

            List<Object> row = new ArrayList<>();
            ComplexTaskMetaManager.ComplexTaskStatus complexTaskStatus =
                tableMeta.getComplexTaskTableMetaBean().getFirstPartStatus();
            if (tableMeta.isGsi()) {
                String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                IndexStatus indexStatus = tableMeta.getGsiTableMetaBean().gsiMetaBean.indexStatus;
                IndexVisibility indexVisibility = tableMeta.getGsiTableMetaBean().gsiMetaBean.visibility;
                row.add(primaryTableName);
                row.add(tableName);
                row.add(complexTaskStatus);
                row.add(indexStatus);
                row.add(indexVisibility);
            } else {
                row.add(tableName);
                row.add("-");
                row.add(complexTaskStatus);
                row.add("-");
                row.add("-");
            }

            result.addRow(row.toArray());
        }

        return result;
    }
}
