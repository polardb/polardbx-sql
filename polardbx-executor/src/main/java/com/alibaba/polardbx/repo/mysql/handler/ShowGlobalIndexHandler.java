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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowGlobalIndex;

import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class ShowGlobalIndexHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(ShowGlobalIndexHandler.class);

    public ShowGlobalIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor resultCursor = buildResultCursor();

        SqlShowGlobalIndex showGlobalIndex = (SqlShowGlobalIndex) ((LogicalDal) logicalPlan).getNativeSqlNode();
        SqlIdentifier tableName = (SqlIdentifier) showGlobalIndex.getTable();
        String schemaName = (tableName != null && 2 == tableName.names.size()) ? tableName.names.get(0) :
            executionContext.getSchemaName();
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (null == executionContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        GsiMetaManager metaManager = executorContext.getGsiManager().getGsiMetaManager();

        GsiMetaManager.GsiMetaBean meta;
        if (null == showGlobalIndex.getTable()) {
            meta = metaManager.getAllGsiMetaBean(schemaName);
        } else {
            meta = metaManager.getTableAndIndexMeta(((SqlIdentifier) showGlobalIndex.getTable()).getLastName(),
                IndexStatus.ALL);
        }

        for (GsiMetaManager.GsiTableMetaBean bean : meta.getTableMeta().values()) {
            if (bean.gsiMetaBean != null && !bean.gsiMetaBean.columnarIndex) {
                GsiMetaManager.GsiIndexMetaBean bean1 = bean.gsiMetaBean;
                Object[] row = new Object[] {
                    bean1.tableSchema, bean1.tableName, bean1.nonUnique ? 1 : 0, bean1.indexName,
                    bean1.indexColumns.stream().map(col -> col.columnName).collect(Collectors.joining(", ")),
                    bean1.coveringColumns.stream().map(col -> col.columnName).collect(Collectors.joining(", ")),
                    bean1.indexType, bean.dbPartitionKey, bean.dbPartitionPolicy, bean.dbPartitionCount,
                    bean.tbPartitionKey, bean.tbPartitionPolicy, bean.tbPartitionCount, bean1.indexStatus};
                resultCursor.addRow(row);
            }
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("GLOBAL_INDEXES");

        resultCursor.addColumn("SCHEMA", DataTypes.StringType);
        resultCursor.addColumn("TABLE", DataTypes.StringType);
        resultCursor.addColumn("NON_UNIQUE", DataTypes.StringType);
        resultCursor.addColumn("KEY_NAME", DataTypes.StringType);
        resultCursor.addColumn("INDEX_NAMES", DataTypes.StringType);
        resultCursor.addColumn("COVERING_NAMES", DataTypes.StringType);
        resultCursor.addColumn("INDEX_TYPE", DataTypes.StringType);
        resultCursor.addColumn("DB_PARTITION_KEY", DataTypes.StringType);
        resultCursor.addColumn("DB_PARTITION_POLICY", DataTypes.StringType);
        resultCursor.addColumn("DB_PARTITION_COUNT", DataTypes.StringType);
        resultCursor.addColumn("TB_PARTITION_KEY", DataTypes.StringType);
        resultCursor.addColumn("TB_PARTITION_POLICY", DataTypes.StringType);
        resultCursor.addColumn("TB_PARTITION_COUNT", DataTypes.StringType);
        resultCursor.addColumn("STATUS", DataTypes.StringType);

        resultCursor.initMeta();

        return resultCursor;
    }

}
