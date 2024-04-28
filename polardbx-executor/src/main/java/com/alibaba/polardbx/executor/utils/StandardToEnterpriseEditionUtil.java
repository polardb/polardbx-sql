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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.topology.CreateDbInfo;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.JdbcUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowTablesMyHandler;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.list.TreeList;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class StandardToEnterpriseEditionUtil {

    protected static Connection buildJdbcConnectionByInstantId(String instantId, String phyDb) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(instantId);
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String ip = ipAndPort.getKey();
        Integer port = ipAndPort.getValue();
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String storageConnProps = haSwitchParams.storageConnPoolConfig.connProps;

        Map<String, String> connPropsMap = GmsJdbcUtil.getPropertiesMapFromAtomConnProps(storageConnProps);
        String socketTimeoutStrOfConn = connPropsMap.get("socketTimeout");
        long socketTimeoutValOfConn = socketTimeoutStrOfConn != null ? Long.valueOf(socketTimeoutStrOfConn) : -1;
        String connProps = GmsJdbcUtil
            .getJdbcConnPropsFromPropertiesMap(GmsJdbcUtil.getDefaultConnPropertiesForGroup(socketTimeoutValOfConn));

        return JdbcUtil.buildJdbcConnection(ip, port, phyDb, user, passwdEnc, connProps);
    }

    //获取imported的database的物理库的连接
    protected static Connection buildJdbcConnectionOnImportedDatabase(String logicalDatabase) throws SQLException {
        DbGroupInfoManager dbGroupInfoManager = DbGroupInfoManager.getInstance();
        List<DbGroupInfoRecord> groupInfoRecords = dbGroupInfoManager.queryGroupInfoBySchema(logicalDatabase);
        if (groupInfoRecords.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                "database [%s] is not a imported database", logicalDatabase
            ));
        }

        String groupName = groupInfoRecords.get(0).groupName;
        ExecutorContext executorContext = ExecutorContext.getContext(logicalDatabase);
        if (executorContext != null) {
            IGroupExecutor groupExecutor = executorContext.getTopologyHandler().get(groupName);
            if (groupExecutor != null && groupExecutor.getDataSource() instanceof TGroupDataSource) {
                TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();
                return dataSource.getConnection(MasterSlave.MASTER_ONLY);
            }
        }
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
            "get physical connection failed, database [%s]", logicalDatabase
        ));
    }

    public static String queryPhyDbNameByLogicalDbName(String logicalDb) {
        DbGroupInfoManager dbGroupInfoManager = DbGroupInfoManager.getInstance();
        List<DbGroupInfoRecord> groupInfoRecords = dbGroupInfoManager.queryGroupInfoBySchema(logicalDb);
        if (groupInfoRecords.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                "database [%s] is not a imported database", logicalDb
            ));
        }
        return groupInfoRecords.get(0).phyDbName;
    }

    public static Map<String, String> queryDatabaseSchemata(String instantId, String phyDb) {
        final String querySql = "select * from information_schema.SCHEMATA where schema_name= '%s'";

        Map<String, String> result = new TreeMap<>(String::compareToIgnoreCase);

        String sql = String.format(querySql, phyDb);
        try (Connection connection = buildJdbcConnectionByInstantId(instantId, "information_schema");
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String schemaName = rs.getString("SCHEMA_NAME");
                String charset = rs.getString("DEFAULT_CHARACTER_SET_NAME");
                String collate = rs.getString("DEFAULT_COLLATION_NAME");
                String encryption = null;

                //only for mysql 8.0
                try {
                    if (rs.findColumn("DEFAULT_ENCRYPTION") > 0) {
                        encryption = rs.getString("DEFAULT_ENCRYPTION");
                    }

                } catch (SQLException e) {

                }
                result.put("SCHEMA_NAME", schemaName);
                result.put("DEFAULT_CHARACTER_SET_NAME", charset);
                result.put("DEFAULT_COLLATION_NAME", collate);
                result.put("DEFAULT_ENCRYPTION", encryption);
            }
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query information from instant [%s]", instantId));
        }

        return result;
    }

    public static Set<String> queryPhysicalTableListFromPhysicalDabatase(String instantId, String phyDb) {
        final String querySql = "show tables";

        List<String> result = new ArrayList<>();
        try (Connection conn = buildJdbcConnectionByInstantId(instantId, phyDb);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(querySql)
        ) {
            while (rs.next()) {
                String phyTableName = rs.getString(1);
                result.add(phyTableName);
            }
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query tables from instant [%s], phyDb [%s]", instantId, phyDb));
        }

        Set<String> st = new TreeSet<>(String::compareToIgnoreCase);
        st.addAll(result.stream().filter(tb -> !tb.startsWith("__drds_")).collect(Collectors.toSet()));
        return st;
    }

    public static Set<String> getTableNamesFromLogicalDatabase(String schemaName, ExecutionContext executionContext) {
        ExecutionContext copiedContext = executionContext.copy();
        copiedContext.setSchemaName(schemaName);
        SqlShowTables sqlShowTables =
            SqlShowTables.create(SqlParserPos.ZERO, false, null, schemaName, null, null, null, null);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showTablesPlan = Planner.getInstance().getPlan(sqlShowTables, plannerContext);
        LogicalShow logicalShowTables = (LogicalShow) showTablesPlan.getPlan();

        IRepository sourceRepo = ExecutorContext
            .getContext(schemaName)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowTablesMyHandler logicalShowTablesMyHandler = new LogicalShowTablesMyHandler(sourceRepo);

        Cursor showTablesCursor =
            logicalShowTablesMyHandler.handle(logicalShowTables, copiedContext);

        Set<String> tables = new TreeSet<>(String::compareToIgnoreCase);
        Row showTablesResult = null;
        while ((showTablesResult = showTablesCursor.next()) != null) {
            if (showTablesResult.getColNum() >= 1 && showTablesResult.getString(0) != null) {
                tables.add(showTablesResult.getString(0));
            } else {
                new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "get tables name in reference database failed.");
            }
        }
        return tables;
    }

    public static Set<String> queryTableWhichHasSequence(String schemaName, ExecutionContext executionContext) {
        final String querySql = "show sequences";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager()
            .executeQuerySql(
                querySql,
                schemaName,
                null
            );

        Set<String> tables = new TreeSet<>(String::compareToIgnoreCase);
        for (Map<String, Object> row : result) {
            String seqName = (String) row.get("NAME");
            if (!StringUtil.isNullOrEmpty(seqName) && seqName.startsWith(AUTO_SEQ_PREFIX)
                && seqName.length() > AUTO_SEQ_PREFIX.length()) {
                String tbName = seqName.substring(AUTO_SEQ_PREFIX.length());
                tables.add(tbName);
            }
        }
        return tables;
    }

    public static boolean logicalCheckTable(String logicalTableName, String logicalDatabase) {
        final String querySql = "check table `%s`";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager()
            .executeQuerySql(
                String.format(querySql, logicalTableName),
                logicalDatabase,
                null
            );

        for (Map<String, Object> checkResultRow : result) {
            String msg_type = (String) checkResultRow.get("MSG_TYPE");
            String msg_text = (String) checkResultRow.get("MSG_TEXT");
            if ("status".equalsIgnoreCase(msg_type) && "OK".equalsIgnoreCase(msg_text)) {
                continue;
            } else {
                return false;
            }
        }

        return true;
    }

    public static String queryCreateTableSql(String instantId, String phyDb, String phyTable) {
        final String querySql = "show create table `%s`";

        String result = null;
        try (Connection conn = buildJdbcConnectionByInstantId(instantId, phyDb);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(querySql, phyTable));
        ) {
            while (rs.next()) {
                result = rs.getString(2);
            }
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query definition of table [%s]", phyTable));
        }
        return result;
    }

    public static CreateDbInfo initCreateDbInfo(String logicalDbName, String phyDbName, String charset, String collate,
                                                Boolean encryption,
                                                LocalityDesc locality,
                                                Predicate<StorageInfoRecord> localityFilter,
                                                int dbType,
                                                long socketTimeout,
                                                boolean ifNotExistTag) {
        CreateDbInfo createDbInfo = DbTopologyManager.initCreateDbInfoForImportDatabase(
            logicalDbName, charset, collate, encryption, locality, localityFilter, dbType, ifNotExistTag, socketTimeout,
            1
        );

        //rectify phyDb name on new topology
        for (Map.Entry<String, String> entry : createDbInfo.getGroupPhyDbMap().entrySet()) {
            entry.setValue(phyDbName);
        }

        return createDbInfo;
    }

    public static String buildGroupName(String logicalDbName) {
        String groupName = String.format(GroupInfoUtil.GROUP_NAME_FOR_IMPORTED_DATABASE, logicalDbName);
        return groupName.toUpperCase();
    }

    public static String normalizePhyTableStructure(String tableName, String createTableSql, String localityExpr) {
        MySqlCreateTableStatement createTableStatement =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(createTableSql).get(0);

        //check if contains primary key
        boolean findPk = false;
        List<SQLTableElement> tableElementList = createTableStatement.getTableElementList();
        for (SQLTableElement element : tableElementList) {
            if (element instanceof MySqlPrimaryKey) {
                findPk = true;
                break;
            }
        }
        if (!findPk) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("not found primary key in table `%s`, please add primary key manually", tableName));
        }

        //handle local partition
        List<SQLCommentHint> hints = createTableStatement.getOptionHints();
        Iterator<SQLCommentHint> hintIter = hints.iterator();
        while (hintIter.hasNext()) {
            SQLCommentHint hint = hintIter.next();
            if (hint.getText() != null && hint.getText().toLowerCase().contains("partition")) {
                hintIter.remove();
            }
        }

        //check engine innodb
        List<SQLAssignItem> tableOptions = createTableStatement.getTableOptions();
        for (SQLAssignItem sqlAssignItem : tableOptions) {
            SQLExpr target = sqlAssignItem.getTarget();
            SQLExpr value = sqlAssignItem.getValue();
            if (target instanceof SQLIdentifierExpr
                && ((SQLIdentifierExpr) target).getName() != null
                && ((SQLIdentifierExpr) target).getName().toLowerCase().contains("engine")) {
                String engine = ((SQLIdentifierExpr) value).toString();
                if (engine != null && !"MYISAM".equalsIgnoreCase(engine) && !"innodb".equalsIgnoreCase(engine)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("unsupported engine [%s]", engine));
                }
            }
        }

        //single table
        createTableStatement.setSingle(true);

        //locality
        SQLCharExpr sqlCharExpr = new SQLCharExpr(localityExpr);
        createTableStatement.setLocality(sqlCharExpr);

        return createTableStatement.toString();
    }

    public static Map<String, BigInteger> querySequenceValuesInPhysicalDatabase(String phyDatabase,
                                                                                String logicalDatabase) {
        final String querySql =
            "select table_name, auto_increment from information_schema.TABLES where table_schema = '%s' and table_name not like '__drds_%%'";

        String sql = String.format(querySql, phyDatabase);

        Map<String, BigInteger> sequences = new TreeMap<>(String::compareToIgnoreCase);
        try (Connection conn = buildJdbcConnectionOnImportedDatabase(logicalDatabase);
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
        ) {
            while (rs.next()) {
                String tableName = rs.getString(1);
                BigInteger seq = (BigInteger) rs.getObject(2);
                if (seq == null) {
                    continue;
                }
                sequences.put(tableName, seq);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query auto_increment information from [%s]", phyDatabase));
        }

        return sequences;
    }

    public static void updateSequence(Map<String, BigInteger> sequences, String logicalDatabase,
                                      Map<String, String> result) {
        final String sql = "alter sequence `%s` start with %s ";
        final String autoIncrementPrefix = AUTO_SEQ_PREFIX;

        for (Map.Entry<String, BigInteger> sequence : sequences.entrySet()) {
            String tableName = sequence.getKey();
            BigInteger seqVal = sequence.getValue();
            String updateSql = String.format(sql, autoIncrementPrefix + tableName, seqVal);
            try {
                DdlHelper.getServerConfigManager()
                    .executeQuerySql(
                        updateSql,
                        logicalDatabase,
                        null
                    );
            } catch (Exception e) {
                result.put(tableName, e.getMessage());
            }
        }
    }
}
