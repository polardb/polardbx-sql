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

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowTableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author youtianyu
 */
public class LogicalShowTableInfoHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDbStatusHandler.class);

    public LogicalShowTableInfoHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTableInfo showTableInfo = (SqlShowTableInfo) show.getNativeSqlNode();
        final SqlNode sqlDbName = showTableInfo.getDbName();
        final SqlNode sqlTableName = showTableInfo.getTableName();

        String dbName = executionContext.getAppName().split("@")[0];
        String tableName = RelUtils.lastStringValue(sqlTableName);
        String schemaName = show.getSchemaName();
        if (DbInfoManager.getInstance().isNewPartitionDb(dbName)) {
            return handleNewPartitionTable(executionContext, schemaName, tableName);
        } else {
            return hanleOldPartitionTable(executionContext, schemaName, tableName);
        }

    }

    private ArrayResultCursor getTableInfoResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("TABLE_INFO");
        result.addColumn("ID", DataTypes.LongType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("SIZE_IN_MB", DataTypes.DoubleType);
        result.initMeta();
        return result;
    }

    private Cursor hanleOldPartitionTable(ExecutionContext executionContext, String schemaName, String tableName) {
        final OptimizerContext context = OptimizerContext.getContext(schemaName);

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        List<TargetDB> topology =
            context.getRuleManager().shard(tableName, true, true, null, null, calcParams, executionContext);

        Map<String, Set<String>> topologyMap = new HashMap<>();

        for (TargetDB targetDB : topology) {
            topologyMap.put(targetDB.getDbIndex(), targetDB.getTableNames());
        }

        MyDataSourceGetter dsGetter = new MyDataSourceGetter(executionContext.getSchemaName());
        int rowCnt = 0;

        TopologyHandler topologyHandler =
            ExecutorContext.getContext(executionContext.getSchemaName()).getTopologyHandler();
        Map<String, Map<String, Object>> resultMap = new HashMap<>();

        for (Group group : topologyHandler.getMatrix().getGroups()) {
            String groupName = group.getName();
            if (!topologyMap.keySet().contains(groupName)) {
                continue;
            }
            TGroupDataSource groupDataSource = dsGetter.getDataSource(groupName);

            StringBuilder sql = new StringBuilder(
                "SELECT TABLE_SCHEMA, TABLE_NAME, (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN (");
            for (String phyTableName : topologyMap.get(groupName)) {
                sql.append("'" + phyTableName + "',");
            }

            sql.deleteCharAt(sql.length() - 1);
            sql.append(");");

            Connection conn = null;
            ResultSet rs = null;
            PreparedStatement ps = null;
            try {
                conn = groupDataSource.getConnection();
                ps = conn.prepareStatement(sql.toString());
                rs = ps.executeQuery();

                while (rs.next()) {
                    String resultTableSchema = rs.getString(1);
                    String resultTableName = rs.getString(2);
                    Object resultSize = rs.getObject(3);

                    if (!resultMap.containsKey(resultTableSchema)) {
                        resultMap.put(resultTableSchema, new HashMap<>());
                    }
                    resultMap.get(resultTableSchema).put(resultTableName, resultSize);
                }
            } catch (Throwable t) {
                logger.error(t);
            } finally {
                JdbcUtils.close(rs);
                JdbcUtils.close(ps);
                JdbcUtils.close(conn);
            }
        }

        ArrayResultCursor result = getTableInfoResultCursor();

        for (Map.Entry<String, Map<String, Object>> mapEntry : resultMap.entrySet()) {
            for (Map.Entry<String, Object> entry : mapEntry.getValue().entrySet()) {
                result.addRow(new Object[] {
                    rowCnt++,
                    mapEntry.getKey(),  // group_name
                    entry.getKey(),     // table_name
                    entry.getValue()    // size_in_mb
                });
            }
        }

        return result;
    }

    private Cursor handleNewPartitionTable(ExecutionContext executionContext, String schemaName, String tableName) {
        ArrayResultCursor result = getTableInfoResultCursor();
        final OptimizerContext context = OptimizerContext.getContext(schemaName);

        TableMeta tableMeta = context.getLatestSchemaManager().getTable(tableName);

        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        if (partitionInfo == null) {
            logger.error("Partition table " + tableName + " does not have partition info");
            return result;
        }

        MyDataSourceGetter dsGetter = new MyDataSourceGetter(executionContext.getSchemaName());
        int rowCnt = 0;

        for (Map.Entry<String, Set<String>> phyPartItem : partitionInfo.getTopology().entrySet()) {
            String groupName = phyPartItem.getKey();

            TGroupDataSource groupDataSource = dsGetter.getDataSource(groupName);
            StringBuilder sql = new StringBuilder(
                "SELECT TABLE_SCHEMA, TABLE_NAME, (DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN (");
            for (String phyTable : phyPartItem.getValue()) {
                sql.append(TStringUtil.quoteString(phyTable)).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(");");
            Connection conn = null;
            ResultSet rs = null;
            PreparedStatement ps = null;
            try {
                conn = groupDataSource.getConnection();
                ps = conn.prepareStatement(sql.toString());
                rs = ps.executeQuery();

                while (rs.next()) {
                    result.addRow(new Object[] {
                        rowCnt++,
                        rs.getObject(1),
                        rs.getObject(2),
                        rs.getObject(3)
                    });
                }
            } catch (Throwable t) {
                logger.error(t);
            } finally {
                JdbcUtils.close(rs);
                JdbcUtils.close(ps);
                JdbcUtils.close(conn);
            }
        }

        return result;
    }
}
