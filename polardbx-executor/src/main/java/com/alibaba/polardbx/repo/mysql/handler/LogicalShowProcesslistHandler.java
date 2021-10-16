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

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowProcesslist;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class LogicalShowProcesslistHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowProcesslistHandler.class);

    private static Class showProcesslistSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showProcesslistSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowProcesslistSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowProcesslistHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowProcesslist showProcesslist = (SqlShowProcesslist) show.getNativeSqlNode();

        if (showProcesslist.isPhysical()) {
            return doPhysicalShow(executionContext.getSchemaName(), showProcesslist);
        } else {
            return doLogicalShow(executionContext, showProcesslist);
        }
    }

    private ArrayResultCursor doLogicalShow(ExecutionContext executionContext, SqlShowProcesslist showProcesslist) {
        if (showProcesslistSyncActionClass == null) {
            throw new NotSupportException();
        }

        ArrayResultCursor result = new ArrayResultCursor("processlist");
        result.addColumn("Id", DataTypes.LongType);
        result.addColumn("User", DataTypes.StringType);
        result.addColumn("Host", DataTypes.StringType);
        result.addColumn("db", DataTypes.StringType);
        result.addColumn("Command", DataTypes.StringType);
        result.addColumn("Time", DataTypes.LongType);
        result.addColumn("State", DataTypes.StringType);
        result.addColumn("Info", DataTypes.StringType);
        result.addColumn("TraceId", DataTypes.StringType);
        if (showProcesslist.isFull()) {
            result.addColumn("SQL_TEMPLATE_ID", DataTypes.StringType);
        }

        ISyncAction showProcesslistSyncAction;
        try {
            showProcesslistSyncAction = (ISyncAction) showProcesslistSyncActionClass
                .getConstructor(String.class, boolean.class)
                .newInstance(executionContext.getSchemaName(), showProcesslist.isFull());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());

        }

        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(showProcesslistSyncAction,
            executionContext.getSchemaName());

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }

            for (Map<String, Object> row : nodeRows) {
                Object[] objects = new Object[result.getReturnColumns().size()];
                int columnIndex = 0;
                objects[columnIndex++] = DataTypes.LongType.convertFrom(row.get("Id"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("User"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("Host"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("db"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("Command"));
                objects[columnIndex++] = DataTypes.LongType.convertFrom(row.get("Time"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("State"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("Info"));
                objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("TraceId"));
                if (showProcesslist.isFull()) {
                    objects[columnIndex++] = DataTypes.StringType.convertFrom(row.get("SQL_TEMPLATE_ID"));
                }
                result.addRow(objects);
            }

        }
        return result;
    }

    public ArrayResultCursor doPhysicalShow(String schemaName, SqlShowProcesslist showProcesslist) {
        ArrayResultCursor result = new ArrayResultCursor("processlist");
        result.addColumn("Group", DataTypes.LongType);
        result.addColumn("Atom", DataTypes.LongType);
        result.addColumn("Id", DataTypes.LongType);
        result.addColumn("User", DataTypes.StringType);
        result.addColumn("db", DataTypes.StringType);
        result.addColumn("Command", DataTypes.StringType);
        result.addColumn("Time", DataTypes.LongType);
        result.addColumn("State", DataTypes.StringType);
        result.addColumn("Info", DataTypes.StringType);

        MyRepository repo = (MyRepository) this.repo;
        int groupIndex = 0;
        String sql = showProcesslist.isFull() ? "show full processlist" : "show processlist";

        // 获取所有DN节点的processlist
        List<Group> allGroups = OptimizerContext.getActiveGroups();
        Set<Pair<String, String>> visitedDNSet = new HashSet<>(allGroups.size());
        for (Group group : allGroups) {

            if (!group.getType().equals(GroupType.MYSQL_JDBC)) {
                continue;
            }

            TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(group.getName());
            if (groupDataSource == null) {
                continue;
            }
            List<TAtomDataSource> atoms = groupDataSource.getAtomDataSources();
            for (int atomIndex = 0; atomIndex < atoms.size(); atomIndex++) {
                TAtomDataSource atom = atoms.get(atomIndex);
                if (!visitedDNSet.add(new Pair<>(atom.getHost(), atom.getPort()))) {
                    // 防止不同的库在相同的实例上导致的重复
                    continue;
                }
                Connection conn = null;

                try {
                    final DataSource dataSource = atom.getDataSource();
                    if (dataSource instanceof XDataSource) {
                        conn = dataSource.getConnection();
                    } else {
                        throw new NotSupportException("xdatasource required");
                    }
                    ResultSet rs = conn.createStatement().executeQuery(sql);
                    while (rs.next()) {
                        result.addRow(new Object[] {
                            (long) groupIndex, (long) atomIndex, rs.getLong("Id"),
                            rs.getString("User"), rs.getString("db"),
                            rs.getString("Command"), rs.getLong("Time"), rs.getString("State"),
                            rs.getString("Info")});
                    }
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("error when show processlist", e);
                } finally {
                    if (conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            logger.error(e);
                        }
                    }
                }
            }
            groupIndex++;

        }

        return result;
    }
}
