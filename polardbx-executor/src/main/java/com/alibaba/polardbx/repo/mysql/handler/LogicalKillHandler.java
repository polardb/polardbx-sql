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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKill;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalKillHandler extends HandlerCommon {

    public Logger logger = LoggerFactory.getLogger(LogicalKillHandler.class);
    public final static String ALL = "ALL";
    public final static String SHOW_PROCESSLIST_SQL = "SHOW PROCESSLIST";

    public LogicalKillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal logicalDal = (LogicalDal) logicalPlan;
        final SqlKill kill = (SqlKill) logicalDal.getNativeSqlNode();

        final String schemaName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();

        String processId = RelUtils.stringValue(kill.getProcessId());

        if (processId == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, processId);
        }

        if (TStringUtil.equalsIgnoreCase(ALL, processId)) {
            return killall(schemaName);
        }
        String[] strs = TStringUtil.split(processId, "-");
        if (strs.length != 3) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, processId);
        }
        MyRepository repo = (MyRepository) this.repo;
        Integer groupIndex = null;
        Integer atomIndex = null;
        Long id = null;
        try {
            groupIndex = Integer.valueOf(strs[0]);
            atomIndex = Integer.valueOf(strs[1]);
            id = Long.valueOf(strs[2]);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, "Illegal thread id " + processId);

        }

        if (groupIndex >= OptimizerContext.getContext(schemaName).getMatrix().getGroups().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, processId);
        }
        Group group = OptimizerContext.getContext(schemaName).getMatrix().getGroups().get(groupIndex);
        if (!group.getType().equals(GroupType.MYSQL_JDBC)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, processId);
        }

        TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(group.getName());
        List<TAtomDataSource> atoms = groupDataSource.getAtomDataSources();
        if (atomIndex >= atoms.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, processId);
        }

        TAtomDataSource atom = atoms.get(atomIndex);
        Connection conn = null;

        try {
            final DataSource rawDataSource = atom.getDataSource();
            if (rawDataSource instanceof XDataSource) {
                try (XConnection connection = (XConnection) rawDataSource.getConnection()) {
                    final int affectRow = (int) connection.execUpdate("kill " + id);
                    return new AffectRowCursor(new int[] {affectRow});
                }
            } else {
                throw GeneralUtil.nestedException("Unknown datasource.");
            }
        } catch (SQLException e) {
            if (e.getErrorCode() == 1094) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_THREAD_ID, e, processId);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, e.getMessage());
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("error when kill", e);
                }
            }
        }

    }

    private Cursor killall(String schemaName) {
        MyRepository repo = (MyRepository) this.repo;
        int affectRow = 0;
        for (Group group : OptimizerContext.getContext(schemaName).getMatrix().getGroups()) {

            if (!group.getType().equals(GroupType.MYSQL_JDBC)) {
                continue;
            }
            if (!DbGroupInfoManager.isVisibleGroup(schemaName, group.getName())) {
                continue;
            }

            TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(group.getName());
            Iterator<Map.Entry<TAtomDataSource, Weight>> iterator =
                groupDataSource.getAtomDataSourceWeights().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TAtomDataSource, Weight> entry = iterator.next();
                TAtomDataSource atom = entry.getKey();
                Weight weight = entry.getValue();

                if (!ConfigDataMode.isMasterMode()) {
                    if (weight != null && weight.w >= 0) {
                        continue;
                    }
                }
                Connection conn = null;
                String user = atom.getDsConfHandle().getRunTimeConf().getUserName();
                try {
                    final DataSource dataSource = atom.getDataSource();
                    if (dataSource instanceof XDataSource) {
                        conn = dataSource.getConnection();
                        if (!XConnectionManager.getInstance().isEnableAuth()) {
                            user = XConfig.X_USER;
                        }
                    } else {
                        throw GeneralUtil.nestedException("Unknown datasource.");
                    }
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(SHOW_PROCESSLIST_SQL);
                    List<Long> processIds = new ArrayList<Long>();
                    while (rs.next()) {
                        try {
                            // 只kill 以下连接：
                            // 1.DRDS创建的连接
                            // 2.正在执行查询的连接
                            // 3.非SHOW PROCESSLIST的连接
                            if (TStringUtil.equals(user, rs.getString("User"))
                                && !TStringUtil.containsIgnoreCase(rs.getString("INFO"), SHOW_PROCESSLIST_SQL)) {
                                processIds.add(rs.getLong("Id"));
                            }
                        } catch (SQLException e) {
                            logger.error("error when show processlist", e);
                        }
                    }

                    rs.close();
                    rs = null;

                    for (Long id : processIds) {
                        try {
                            stmt.executeUpdate("KILL " + id);
                            // 没报错，说明kill 成功了
                            // mysql返回的affectrow是0
                            affectRow++;
                        } catch (SQLException e) {
                            logger.warn("error when killall", e);
                        }
                    }

                    stmt.close();
                    stmt = null;

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

        }

        return new AffectRowCursor(new int[] {affectRow});

    }
}
