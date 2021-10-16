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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlShowTables.SqlShowTablesOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenmo.cm
 */
public class LogicalShowTableStatusHandler extends LogicalInfoSchemaQueryHandler {

    public LogicalShowTableStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTableStatus showTableStatus = (SqlShowTableStatus) show.getNativeSqlNode();

        ArrayResultCursor result = new ArrayResultCursor("SOUTINES");
        buildColumns(result);
        result.initMeta();

        String schemaName = show.getSchemaName();
        if (showTableStatus.getDbName() != null && TStringUtil.isNotEmpty(showTableStatus.getDbName().toString())) {
            schemaName = showTableStatus.getDbName().toString();
        }

        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        LogicalInfoSchemaContext infoSchemaContext = new LogicalInfoSchemaContext(executionContext);
        infoSchemaContext.setTargetSchema(schemaName);
        infoSchemaContext.prepareContextAndRepository(repo);

        showTables(showTableStatus.like,
            showTableStatus.getDbName(),
            show,
            infoSchemaContext);

        for (String logicalTableName : infoSchemaContext.getLogicalTableNames()) {
            result.addRow(showSingleTableStatus(logicalTableName, infoSchemaContext));
        }

        return result;
    }

    private void showTables(SqlNode like, SqlNode dbName, LogicalShow showNode,
                            LogicalInfoSchemaContext infoSchemaContext) {
        String schemaName = infoSchemaContext.getTargetSchema();

        final SqlShowTables showTables = SqlShowTables.create(SqlParserPos.ZERO,
            false,
            dbName,
            schemaName,
            like,
            null,
            null,
            null);

        final LogicalShow showTablesNode = LogicalShow.create(
            Show.create(
                showTables,
                SqlShowTablesOperator.getRelDataType(showNode.getCluster().getTypeFactory(), null, false),
                showNode.getCluster()),
            infoSchemaContext.getOptimizerContext().getRuleManager().getDefaultDbIndex(null),
            null,
            schemaName);

        showTables(showTablesNode, infoSchemaContext);
    }

    public static void buildColumns(ArrayResultCursor result) {
        for (int i = 0; i < SqlShowTableStatus.NUM_OF_COLUMNS; i++) {
            DataType dataType;
            switch (SqlShowTableStatus.COLUMN_TYPES.get(i)) {
            case LONG:
                dataType = DataTypes.LongType;
                break;
            case BIGDECIMAL:
                dataType = DataTypes.DecimalType;
                break;
            case STRING:
            default:
                dataType = DataTypes.StringType;
                break;
            }
            result.addColumn(SqlShowTableStatus.COLUMN_NAMES.get(i), dataType);
        }
    }

}
