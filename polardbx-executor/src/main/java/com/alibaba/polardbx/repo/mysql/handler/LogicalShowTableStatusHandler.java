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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ShowTablesSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.ViewsInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlShowTables.SqlShowTablesOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        infoSchemaContext.setWithView(true);
        infoSchemaContext.prepareContextAndRepository(repo);

        showTables(showTableStatus.like,
            showTableStatus.getDbName(),
            show,
            infoSchemaContext);

        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOGICAL_TABLE_META)
            || ConfigDataMode.isColumnarMode()) {
            //access metadb
            List<Object[]> objects = showSingleTableStatusWithColumnarMode(schemaName);
            for (Object[] row : objects) {
                result.addRow(row);
            }
        } else {
            for (String logicalTableName : infoSchemaContext.getLogicalTableNames()) {
                TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
                    .getTableWithNull(logicalTableName);
                result.addRow(showSingleTableStatus(logicalTableName, infoSchemaContext));
            }
        }

        return result;
    }

    protected List<Object[]> showSingleTableStatusWithColumnarMode(String schemaName) {
        List<Object[]> result = new ArrayList<>();
        //get dataSource and Connection
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            //fetch table status
            String sql = "select * from " + SqlIdentifier.surroundWithBacktick(GmsSystemTables.TABLES)
                + "where engine != ? and table_schema = ?";
            Map<Integer, ParameterContext> params = new HashMap<>();
            //we don't show OSS index by default
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, "OSS");
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            List<ShowTablesSchemaRecord> tablesInfoSchemaRecords =
                MetaDbUtil.query(sql, params, ShowTablesSchemaRecord.class, connection);
            for (ShowTablesSchemaRecord tablesInfoSchemaRecord : tablesInfoSchemaRecords) {
                result.add(transTableInfoScheamRecordToObject(tablesInfoSchemaRecord));
            }
            //fetch view status
            String sqlView =
                "select * from " + SqlIdentifier.surroundWithBacktick(GmsSystemTables.VIEWS) + "where schema_name = '"
                    + schemaName + "'";
            List<ViewsInfoRecord> viewsInfoRecords = MetaDbUtil.query(sqlView, ViewsInfoRecord.class, connection);
            for (ViewsInfoRecord viewsInfoRecord : viewsInfoRecords) {
                result.add(transViewsRecordToObject(viewsInfoRecord));
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "fail to access metadb" + e.getMessage());
        }
        return result;
    }

    public Object[] transTableInfoScheamRecordToObject(ShowTablesSchemaRecord info) {
        BigDecimal rowsCopy = BigDecimal.valueOf(info.tableRows), avgRowLengthCopy =
            BigDecimal.valueOf(info.avgRowLength);
        BigDecimal dataLengthCopy = BigDecimal.valueOf(info.dataLength), maxDataLengthCopy =
            BigDecimal.valueOf(info.maxDataLength);
        BigDecimal indexLengthCopy = BigDecimal.valueOf(info.indexLength), dataFreeCopy =
            BigDecimal.valueOf(info.dataFree);

        return new Object[] {
            info.tableName, info.engine, info.version, info.rowFormat, rowsCopy, avgRowLengthCopy, dataLengthCopy,
            maxDataLengthCopy, indexLengthCopy, dataFreeCopy, info.autoIncrement, info.createTime, info.updateTime,
            info.checkTime, info.tableCollation,
            info.checkSum, info.createOptions, info.tableComment};
    }

    public Object[] transViewsRecordToObject(ViewsInfoRecord info) {
        return new Object[] {
            info.viewName, null, 0, null, 0, 0, 0, 0, 0, 0, 0, null, null, null, null, null, null, "VIEW"};
    }

    void showTables(SqlNode like, SqlNode dbName, LogicalShow showNode,
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
            result.addColumn(SqlShowTableStatus.COLUMN_NAMES.get(i), dataType, false);
        }
    }

}
