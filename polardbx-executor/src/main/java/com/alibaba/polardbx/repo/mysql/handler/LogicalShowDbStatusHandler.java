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
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowDbStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author chenmo.cm
 */
public class LogicalShowDbStatusHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDbStatusHandler.class);

    public LogicalShowDbStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowDbStatus showDbStatus = (SqlShowDbStatus) show.getNativeSqlNode();
        ArrayResultCursor result = new ArrayResultCursor("DB_STATUS");
        result.addColumn("ID", DataTypes.LongType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("CONNECTION_STRING", DataTypes.StringType);
        result.addColumn("PHYSICAL_DB", DataTypes.StringType);
        if (showDbStatus.isFull()) {
            result.addColumn("PHYSICAL_TABLE", DataTypes.StringType);
        }
        result.addColumn("SIZE_IN_MB", DataTypes.DoubleType);
        result.addColumn("RATIO", DataTypes.StringType);
        result.addColumn("THREAD_RUNNING", DataTypes.StringType);
        result.initMeta();

        addSchemaStatus(executionContext, executionContext.getSchemaName(), showDbStatus, result);
        return result;
    }

    private void addSchemaStatus(ExecutionContext executionContext,
                                 String schemaName,
                                 SqlShowDbStatus showDbStatus,
                                 ArrayResultCursor result) {
        TopologyHandler topologyHandler = ExecutorContext.getContext(schemaName).getTopologyHandler();
        MiddleStatusBean mStatus = new MiddleStatusBean(topologyHandler.getAppName(),
            showDbStatus.isFull(),
            handlePattern(null == showDbStatus.like ? null : RelUtils.stringValue(showDbStatus.like)));

        MyDataSourceGetter dsGetter = new MyDataSourceGetter(schemaName);
        for (Group group : topologyHandler.getMatrix().getGroups()) {
            TGroupDataSource ds = dsGetter.getDataSource(group.getName());

            final Map<String, DataSourceWrapper> dataSourceWrapperMap = ds.getConfigManager()
                .getDataSourceWrapperMap();

            for (Entry<String, DataSourceWrapper> atomEntry : dataSourceWrapperMap.entrySet()) {
                final DataSourceWrapper dataSourceWrapper = atomEntry.getValue();
                final Weight weightVal = dataSourceWrapper.getWeight();
                int w = -1;
                int r = -1;
                if (weightVal != null) {
                    w = weightVal.w;
                    r = weightVal.r;
                }

                if (ConfigDataMode.isMasterMode()) {
                        if (!(w > 0)) {
                            continue;
                        }
                    } else {
                        if (!(r > 0 && w <= 0)) {
                            continue;
                        }
                    }

                final TAtomDataSource atomDs = dataSourceWrapper.getWrappedDataSource();
                final TAtomDsConfDO atomConfig = atomDs.getDsConfHandle().getRunTimeConf();

                mStatus.addGroup(atomConfig.getIp() + ":" + atomConfig.getPort(),
                    group,
                    atomDs,
                    atomConfig.getDbName());
            } // end of for
        } // end of for

        mStatus.finishAllInstance(result, executionContext);
    }

    private String handlePattern(String pattern) {
        String result = pattern;
        if (TStringUtil.isBlank(pattern)) {
            return pattern;
        }

        if (TStringUtil.startsWith(pattern, "\'") && TStringUtil.endsWith(pattern, "\'")) {
            result = TStringUtil.substring(pattern, 1, pattern.length() - 1);
        } else if (TStringUtil.startsWith(pattern, "\"") && TStringUtil.endsWith(pattern, "\"")) {
            result = TStringUtil.substring(pattern, 1, pattern.length() - 1);
        }

        if (!TStringUtil.endsWith(result, "%")) {
            result += "%";
        }

        return result;
    }

    private TAtomDataSource getAtomDatasource(DataSource s) {
        if (s instanceof TAtomDataSource) {
            return (TAtomDataSource) s;
        }

        if (s instanceof DataSourceWrapper) {
            return getAtomDatasource(((DataSourceWrapper) s).getWrappedDataSource());
        }

        throw new IllegalAccessError();
    }

    private class MiddleStatusBean {

        private boolean full = false;
        private String pattern = "";
        private String resultName = "";
        private Map<String, LogicalShowDbStatusHandler.InstanceStatusBean> resultRows =
            new LinkedHashMap<String, LogicalShowDbStatusHandler.InstanceStatusBean>();

        public MiddleStatusBean(String resultName, boolean full, String pattern) {
            this.resultName = resultName;
            this.full = full;
            this.pattern = pattern;
        }

        /**
         * 保存一个分库的基本信息
         */
        public LogicalShowDbStatusHandler.MiddleStatusBean addGroup(String connectionString, Group headGroup,
                                                                    TAtomDataSource headDS, String physicalDbName) {
            if (!resultRows.containsKey(connectionString)) {
                beginInstance(resultName, connectionString, headGroup, headDS);
            }

            resultRows.get(connectionString).addGroup(new LogicalShowDbStatusHandler.DbTableStatusBean(resultName,
                connectionString,
                physicalDbName));

            return this;
        }

        /**
         * 增加一个新的实例
         */
        public LogicalShowDbStatusHandler.MiddleStatusBean beginInstance(String appname, String connectionString,
                                                                         Group headGroup, TAtomDataSource headDS) {
            resultRows.put(connectionString, new LogicalShowDbStatusHandler.InstanceStatusBean(appname,
                connectionString,
                headGroup.getName(),
                headDS));

            return this;
        }

        public LogicalShowDbStatusHandler.MiddleStatusBean finishAllInstance(ArrayResultCursor resultCursor,
                                                                             ExecutionContext executionContext) {
            for (Entry<String, LogicalShowDbStatusHandler.InstanceStatusBean> instanceEntry : resultRows.entrySet()) {
                LogicalShowDbStatusHandler.InstanceStatusBean instance = instanceEntry.getValue();
                instance.finishInstance(resultCursor, executionContext, full, pattern);
            }

            return this;
        }
    }

    private class InstanceStatusBean {

        private final String resultName;
        private final String resultConnectionString;
        private final String headGroup;
        private List<LogicalShowDbStatusHandler.DbTableStatusBean> resultRows =
            new ArrayList<LogicalShowDbStatusHandler.DbTableStatusBean>();
        private TAtomDataSource headDS = null;
        private Double totalSize = 0.0d;

        public InstanceStatusBean(String resultName, String resultConnectionString, String headGroup,
                                  TAtomDataSource headDS) {
            this.resultName = resultName;
            this.resultConnectionString = resultConnectionString;
            this.headGroup = headGroup;
            this.headDS = headDS;
        }

        public LogicalShowDbStatusHandler.InstanceStatusBean addGroup(
            LogicalShowDbStatusHandler.DbTableStatusBean dbStatus) {
            resultRows.add(dbStatus);
            return this;
        }

        public LogicalShowDbStatusHandler.InstanceStatusBean finishInstance(ArrayResultCursor resultCursor,
                                                                            ExecutionContext executionContext,
                                                                            boolean full, String pattern) {
            int currentId = resultCursor.getRows().size() + 1;

            if (full) {
                fillPropertiesFull(pattern, executionContext);
            } else {
                fillProperties(pattern, executionContext);
            }

            for (LogicalShowDbStatusHandler.DbTableStatusBean row : resultRows) {
                row.setId(currentId);
                currentId++;

                // 计算比例
                if (!("100%".equals(row.getRatio()) && "TOTAL".equals(row.getPhysicalDb()))) {
                    if (this.totalSize > 0.0d) {
                        double ratio = row.getSizeInMb() / this.totalSize;
                        row.setRatio(new DecimalFormat("#.##%").format(ratio));
                    }
                }

                resultCursor.addRow(row.toObjects(full));
            }

            return this;
        }

        private List<LogicalShowDbStatusHandler.DbTableStatusBean> fillPropertiesFull(String pattern,
                                                                                      ExecutionContext executionContext) {
            Connection connection = null;
            try {
                connection = getConnection();
                String threadRunning = getThreadRuning(connection);
                Double resultTotalSize = 0.0;

                List<LogicalShowDbStatusHandler.DbTableStatusBean> tmpDbResultRows =
                    new LinkedList<LogicalShowDbStatusHandler.DbTableStatusBean>();
                for (LogicalShowDbStatusHandler.DbTableStatusBean row : this.resultRows) {
                    Map<String, Double> tableSizeMap = getTableSizeMap(row.getPhysicalDb(), pattern, connection);

                    Double dbSize = 0.0d;
                    List<LogicalShowDbStatusHandler.DbTableStatusBean> tmpTableResultRows =
                        new LinkedList<LogicalShowDbStatusHandler.DbTableStatusBean>();
                    for (Entry<String, Double> tableEntry : tableSizeMap.entrySet()) {
                        LogicalShowDbStatusHandler.DbTableStatusBean tableResult =
                            new LogicalShowDbStatusHandler.DbTableStatusBean(row.getName(),
                                row.getConnectionString(),
                                row.physicalDb,
                                tableEntry.getKey(),
                                tableEntry.getValue());
                        tmpTableResultRows.add(tableResult);
                        dbSize += tableResult.getSizeInMb();
                    }

                    if (tmpTableResultRows.size() > 1) {
                        tmpDbResultRows.add(new LogicalShowDbStatusHandler.DbTableStatusBean(this.resultName,
                            this.resultConnectionString,
                            row.getPhysicalDb(),
                            "TOTAL",
                            dbSize));
                    }

                    tmpDbResultRows.addAll(tmpTableResultRows);
                    resultTotalSize += dbSize;
                } // end of for

                tmpDbResultRows.add(0, new LogicalShowDbStatusHandler.DbTableStatusBean(this.resultName,
                    this.resultConnectionString,
                    "TOTAL").setRatio("100%").setSizeInMb(resultTotalSize).setThreadRunning(threadRunning));

                this.totalSize = resultTotalSize;
                this.resultRows = tmpDbResultRows;
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException("Get Db Status from " + headGroup + " failed!", e);
            } finally {
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
            }

            return resultRows;
        }

        /**
         * 填充分库属性
         */
        private List<LogicalShowDbStatusHandler.DbTableStatusBean> fillProperties(String pattern,
                                                                                  ExecutionContext executionContext) {
            Set<String> dbNameSet = new LinkedHashSet<String>();
            for (LogicalShowDbStatusHandler.DbTableStatusBean row : resultRows) {
                if (!("100%".equals(row.getRatio()) && "TOTAL".equals(row.getPhysicalDb()))) {
                    dbNameSet.add(row.getPhysicalDb());
                }
            }

            Connection connection = null;
            try {
                connection = getConnection();
                Map<String, Double> dbSizeMap = getDbSizeMap(dbNameSet, pattern, connection);
                String threadRunning = getThreadRuning(connection);

                Double resultTotalSize = 0.0;
                List<LogicalShowDbStatusHandler.DbTableStatusBean> tmpResultRows =
                    new LinkedList<LogicalShowDbStatusHandler.DbTableStatusBean>();
                for (LogicalShowDbStatusHandler.DbTableStatusBean row : this.resultRows) {
                    Double size = dbSizeMap.get(row.getPhysicalDb());
                    if ("100%".equals(row.getRatio()) && "TOTAL".equals(row.getPhysicalDb())) {
                        row.setThreadRunning(threadRunning);
                        tmpResultRows.add(row);
                    } else {
                        if (size == null) {
                            continue;
                        }
                        row.setSizeInMb(size);
                        resultTotalSize += row.getSizeInMb();
                        tmpResultRows.add(row);
                    }
                } // end of for

                tmpResultRows.add(0, new LogicalShowDbStatusHandler.DbTableStatusBean(this.resultName,
                    this.resultConnectionString,
                    "TOTAL").setThreadRunning(threadRunning).setRatio("100%").setSizeInMb(resultTotalSize));

                this.resultRows = tmpResultRows;
                this.totalSize = resultTotalSize;
            } catch (SQLException e) {
                throw new TddlNestableRuntimeException("Get Db Status from " + headGroup + " failed!", e);
            } finally {
                if (null != connection) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
            }

            return resultRows;
        }

        private String getThreadRuning(Connection conn) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = conn.prepareStatement("SHOW STATUS LIKE \"Threads_running\"");
                rs = ps.executeQuery();

                String result = "";
                while (rs.next()) {
                    result = rs.getString(2);
                    break;
                }

                return result;
            } catch (SQLException e) {
                logger.error("", e);
            } finally {
                if (null != rs) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
                if (null != ps) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
            }
            return "";
        }

        private Map<String, Double> getDbSizeMap(Set<String> dbNameSet, String pattern, Connection conn) {
            Map<String, Double> dbSizeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            StringBuilder sql = new StringBuilder("SELECT table_schema \"Data Base Name\",  sum( data_length + "
                + "index_length ) / 1024 / 1024 \"Data Base Size in MB\" FROM information_schema.TABLES WHERE "
                + "table_schema in (");
            Iterator<String> it = dbNameSet.iterator();
            List<String> parameters = new LinkedList<String>();
            while (it.hasNext()) {
                parameters.add(it.next());
                sql.append("?");
                if (it.hasNext()) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (TStringUtil.isNotBlank(pattern)) {
                sql.append(" AND table_schema like ?");
                parameters.add(pattern);
            }
            sql.append(" GROUP BY table_schema;");

            doQuery(conn, dbSizeMap, sql, parameters);

            return dbSizeMap;
        }

        private Map<String, Double> getTableSizeMap(String tableSchema, String pattern, Connection conn) {
            Map<String, Double> dbSizeMap = new HashMap<String, Double>();

            StringBuilder sql = new StringBuilder("SELECT table_name \"Table Name\",  sum( data_length + "
                + "index_length ) / 1024 / 1024 \"Data Base Size in MB\" FROM information_schema.TABLES WHERE "
                + "table_schema = ?");
            List<String> parameters = new LinkedList<String>();
            parameters.add(tableSchema);
            if (TStringUtil.isNotBlank(pattern)) {
                sql.append(" AND (table_schema like ? or table_name like ?)");
                parameters.add(pattern);
                parameters.add(pattern);
            }
            sql.append(" GROUP BY table_schema, table_name;");

            doQuery(conn, dbSizeMap, sql, parameters);

            return dbSizeMap;
        }

        private void doQuery(Connection conn, Map<String, Double> dbSizeMap, StringBuilder sql,
                             List<String> parameters) {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = conn.prepareStatement(sql.toString());
                ps.setQueryTimeout(120);

                for (int index = 0; index < parameters.size(); index++) {
                    ps.setString(index + 1, parameters.get(index));
                }

                rs = ps.executeQuery();

                while (rs.next()) {
                    String dbName = rs.getString(1);
                    Double sizeInMb = rs.getDouble(2);
                    dbSizeMap.put(dbName, sizeInMb);
                }
            } catch (SQLException e) {
                logger.error("", e);
            } finally {
                if (null != rs) {
                    try {
                        rs.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
                if (null != ps) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                    }
                }
            }
        }

        private Connection getConnection() throws SQLException {
            return headDS.getDataSource().getConnection();
        }

        public String getResultConnectionString() {
            return resultConnectionString;
        }

        public String getHeadGroup() {
            return headGroup;
        }

        public List<LogicalShowDbStatusHandler.DbTableStatusBean> getResultRows() {
            return resultRows;
        }

        public TAtomDataSource getHeadDS() {
            return headDS;
        }
    }

    private class DbTableStatusBean {

        private long id = 1;
        private String name;
        private String connectionString;
        private String physicalDb = "";
        private String physicalTable = "";
        private double sizeInMb = 0.0;
        private String ratio = "";
        private String threadRunning = "";

        public DbTableStatusBean(String name, String connectionString, String physicalDb, String physicalTable,
                                 double sizeInMb) {
            this.name = name;
            this.connectionString = connectionString;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.sizeInMb = sizeInMb;
        }

        public DbTableStatusBean(String name, String connectionString, String physicalDb) {
            this.name = name;
            this.connectionString = connectionString;
            this.physicalDb = physicalDb;
        }

        public Object[] toObjects(boolean full) {
            if (full) {
                return new Object[] {
                    id, name, connectionString, physicalDb, physicalTable, sizeInMb, ratio,
                    threadRunning};
            } else {
                return new Object[] {id, name, connectionString, physicalDb, sizeInMb, ratio, threadRunning};
            }
        }

        public long getId() {
            return id;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setId(long id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setName(String name) {
            this.name = name;
            return this;
        }

        public String getConnectionString() {
            return connectionString;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public String getPhysicalDb() {
            return physicalDb;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setPhysicalDb(String physicalDb) {
            this.physicalDb = physicalDb;
            return this;
        }

        public String getPhysicalTable() {
            return physicalTable;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setPhysicalTable(String physicalTable) {
            this.physicalTable = physicalTable;
            return this;
        }

        public double getSizeInMb() {
            return sizeInMb;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setSizeInMb(double sizeInMb) {
            this.sizeInMb = sizeInMb;
            return this;
        }

        public String getRatio() {
            return ratio;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setRatio(String ratio) {
            this.ratio = ratio;
            return this;
        }

        public String getThreadRunning() {
            return threadRunning;
        }

        public LogicalShowDbStatusHandler.DbTableStatusBean setThreadRunning(String threadRunning) {
            this.threadRunning = threadRunning;
            return this;
        }
    }
}
