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

package com.alibaba.polardbx.server;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.audit.ConnectionInfo;
import com.alibaba.polardbx.common.constants.CpuStatAttribute;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ShareReadViewPolicy;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.ExceptionUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.support.LogFormat;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.TPreparedStatement;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.FrontendAuthenticator;
import com.alibaba.polardbx.net.handler.FrontendAuthorityAuthenticator;
import com.alibaba.polardbx.net.handler.LoadDataHandler;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.ccl.service.Reschedulable;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.LoadDataContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.core.planner.MetaConverter;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreStmtMetaData;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.alibaba.polardbx.optimizer.planmanager.StatementMap;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.alibaba.polardbx.server.executor.utils.BinaryResultSetUtil;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;
import com.alibaba.polardbx.server.executor.utils.ResultSetUtil;
import com.alibaba.polardbx.server.handler.ServerLoadDataHandler;
import com.alibaba.polardbx.server.mock.MockExecutor;
import com.alibaba.polardbx.server.response.Ping;
import com.alibaba.polardbx.server.session.ServerSession;
import com.alibaba.polardbx.server.ugly.hint.EagleeyeTestHintParser;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.server.util.MockUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.transaction.ReadOnlyTsoTransaction;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSystemVar;
import org.apache.calcite.sql.SqlUserDefVar;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.alibaba.polardbx.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_DEADLOCK;
import static com.alibaba.polardbx.common.utils.ExceptionUtils.isMySQLIntegrityConstraintViolationException;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.vendorErrorIs;

/**
 * @author xianmao.hexm 2011-4-21 上午11:22:57
 */
public final class ServerConnection extends FrontendConnection implements Reschedulable {

    private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);
    private static final Logger io_logger = LoggerFactory.getLogger("net_error");
    private static final ErrorPacket shutDownError = PacketUtil.getShutdown();
    private static final long AUTH_TIMEOUT = 15 * 1000L;
    private static final long CACHE_TIMEOUT = 15 * 1000L;
    private static final ClusterAcceptIdGenerator g = new ClusterAcceptIdGenerator();
    private static final long MAX_VALUE = 0xffffffffL;
    /**
     * sql是否正在执行
     */
    private final AtomicBoolean statementExecuting = new AtomicBoolean(false);
    private final Object mdlContextLock = new Object();
    /**
     * <pre>
     * Id of physical sql group, example:
     *
     * create table t1 (id int, name varchar(64), primary key(id)) dbpartition by hash(name);
     * insert into t1 (id, name) value(1, "alic"), (2, "bob"), (3, "alc");
     * update t1 set name = "alice" where name in ("alic", "alc");
     *
     * update will be converted to select + delete + insert like
     *
     * select id, name from t1 where name in ("alic", "alc") for update;
     * delete from t1 where id in (1,3);
     * insert into t1 (id, name) value (1, "alice"), (3, "alice");
     *
     * For cdc, the information "delete executed before insert" is important,
     * so we use a successive physical sql group id to represent which goes first.
     *
     * </pre>
     */
    private Long phySqlId = 0L;
    private Long sqlId = 0L;
    private Long txId = 0L;
    private String sqlSample = null;
    private InternalTimeZone timeZone = null;
    private volatile int txIsolation = -1;
    private volatile int stmtTxIsolation = -1;
    private volatile int socketTimeout = -1;
    private volatile boolean autocommit = true;
    private ITransactionPolicy trxPolicy = null;
    private volatile SchemaConfig schemaConfig = null;
    private volatile Map<String, TbPriv> tablePrivCache = null;
    private volatile Map<String, DbPriv> schemaPrivCache = null;
    // default sql mode
    private volatile String sqlMode = null;
    private ServerSession session;
    private volatile TConnection conn;
    final private AtomicInteger stmtId = new AtomicInteger(0);
    /**
     * smForQuery for COM_QUERY and smForPrepare for COM_STMT_PREPARE, they are
     * independent since COM_QUERY need set @param = xxx, so another param map
     * is needed, COM_STMT_PREPARE no need the param map since it always carry
     * the param on one COM_STMT_EXECUTE command.
     */
    final private StatementMap smForQuery;
    final private StatementMap smForPrepare;

    private boolean beginTransaction = false;
    private MatrixStatistics stats;
    // 支持下set sql_select_limit参数
    private Long sqlSelectLimit = null;
    // 下推到下层的系统变量，全部小写
    final private Map<String, Object> serverVariables = new HashMap<>();
    // 全局的系统变量
    final private Map<String, Object> globalServerVariables = new HashMap<>();
    // 用户定义的变量，全部小写
    final private Map<String, Object> userDefVariables = new HashMap<>();
    // 特殊处理的系统变量以及自定义的系统变量，全部小写
    final private Map<String, Object> extraServerVariables = new HashMap<>();
    // ConnectionParams 中支持的其他系统变量，全部大写
    final private Map<String, Object> connectionVariables = new HashMap();
    private String traceId = null;
    private boolean readOnly = false;
    private boolean enableANSIQuotes = false;
    private long lastSqlStartTime = 0;
    private boolean sqlMock = false;
    private MockExecutor mockExecutor;
    private MdlContext mdlContext;
    private RuntimeStatistics lastSqlRunTimeStat = null;
    private volatile RescheduleParam rescheduleParam;
    private volatile RescheduleTask rescheduleTask;

    /**
     * Session's active roles.
     *
     * @see ActiveRoles
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set Role</a>
     */
    private volatile ActiveRoles activeRoles = ActiveRoles.defaultValue();

    private ShareReadViewPolicy shareReadView = ShareReadViewPolicy.DEFAULT;

    public ServerConnection(SocketChannel channel) {

        super(channel);

        smForQuery = new StatementMap(false);
        smForPrepare = new StatementMap(true);

        // JDBC会改成STRICT_TRANS_TABLES，为与MySQL兼容，需要改成global的设置
        serverVariables.put("sql_mode", "default");
        serverVariables.put("net_write_timeout", (long) (8 * 60 * 60));

        instanceId = CobarServer.getInstance().getConfig().getSystem().getInstanceId();

        instanceId = CobarServer.getInstance().getConfig().getSystem().getInstanceId();

        /**
         * For each front connection mdl context should be create and remove only once.
         * Cause some other component will call TConnection close during ds.init()
         * which will remove mdl context.
         * So we can only create MdlContext as a field of ServerConnection
         */
        mdlContext = MdlManager.addContext(this.id);
        //ExtraServerVariables 初始化
        initExtraServerVariables();

        if (!ConfigDataMode.isMasterMode()) {
            readOnly = true;
        }
    }

    public static int toFlag(ColumnMeta cm) {
        int flags = 0;
        if (cm.isNullable()) {
            flags |= 0001;
        }

        if (cm.isAutoIncrement()) {
            flags |= 0200;
        }

        return flags;
    }

    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    public Map<String, Object> getGlobalServerVariables() {
        return globalServerVariables;
    }

    public Map<String, Object> getUserDefVariables() {
        return userDefVariables;
    }

    public Map<String, Object> getConnectionVariables() {
        return connectionVariables;
    }

    private void initExtraServerVariables() {
        if (extraServerVariables.isEmpty()) {
            //init
            extraServerVariables.put("sockettimeout", this.socketTimeout);
            extraServerVariables.put("pure_async_ddl_mode", false);
            extraServerVariables.put("transaction policy", 3);
            extraServerVariables.put("trans.policy", "ALLOW_READ");
            extraServerVariables.put("drds_transaction_policy", "ALLOW_READ");
            extraServerVariables.put("read", "WRITE");
            extraServerVariables.put("batch_insert_policy", "SPLIT");
            extraServerVariables.put("sql_mock", this.sqlMock);
        }
    }

    public Map<String, Object> getExtraServerVariables() {
        return extraServerVariables;
    }

    public Object getSysVarValue(SqlSystemVar var) {
        Object value = null;
        VariableManager variableManager =
            OptimizerContext.getContext(schema).getVariableManager();
        String lowerCaseKey = var.getName().toLowerCase();

        if (extraServerVariables.containsKey(lowerCaseKey)) {
            value = extraServerVariables.get(lowerCaseKey);
        } else if (serverVariables.containsKey(lowerCaseKey)) {
            value = serverVariables.get(lowerCaseKey);
        } else {
            switch (var.getScope()) {
            case GLOBAL:
                value = variableManager.getGlobalVariable(lowerCaseKey);
                break;
            case SESSION:
                switch (lowerCaseKey) {
                case "last_insert_id":
                    value = getLastInsertId();
                    break;
                case "tx_isolation":
                case "transaction_isolation":
                    // Note: It's hard to get global isolation level here...
                    IsolationLevel isolation = IsolationLevel.fromInt(txIsolation);
                    value = (isolation != null ? isolation.nameWithHyphen() : null);
                    break;
                case "read_only":
                    if (ConfigDataMode.isMasterMode()) {
                        value = 0;
                    } else if (ConfigDataMode.isSlaveMode()) {
                        value = 1;
                    }
                    break;
                default:
                    value = variableManager.getSessionVariable(lowerCaseKey);
                    break;
                }
                break;
            default:
                value = null;
            }
        }
        return value;
    }

    public Object getVarValueBySqlNode(SqlNode oriValue) {
        Object value = null;
        if (oriValue instanceof SqlUserDefVar) {
            String lowerCaseKey = ((SqlUserDefVar) oriValue).getName().toLowerCase();
            value = userDefVariables.get(lowerCaseKey);
        } else if (oriValue instanceof SqlSystemVar) {
            SqlSystemVar var = (SqlSystemVar) oriValue;
            if (!ServerVariables.contains(var.getName()) && !ServerVariables.isExtra(var.getName())) {
                return null;
            }
            value = getSysVarValue(var);
        }
        return value;
    }

    public String getVarStringValue(SqlNode oriValue) {
        String value;
        if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
            value = String.valueOf(this.getVarValueBySqlNode(oriValue));
        } else {
            value = RelUtils.stringValue(oriValue);
        }
        return value;
    }

    public Integer getVarIntegerValue(SqlNode oriValue) {
        int value;
        if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
            value = Integer.parseInt(String.valueOf(this.getVarValueBySqlNode(oriValue)));
        } else {
            value = RelUtils.integerValue((SqlLiteral) oriValue);
        }
        return value;
    }

    public Boolean getVarBooleanValue(SqlNode oriValue) {
        Object value = getVarValueBySqlNode(oriValue);
        if (value == null) {
            return null;
        }
        String strValue = String.valueOf(value);
        Boolean res = false;
        if (value instanceof Boolean) {
            res = (Boolean) value;
        } else if (value instanceof Integer) {
            res = (Integer) value != 0;
        } else if ("ON".equalsIgnoreCase(strValue)) {
            res = true;
        } else if ("OFF".equalsIgnoreCase(strValue)) {
            res = false;
        } else {
            return null;
        }
        return res;
    }

    @Override
    public boolean checkConnectionCount() {
        int maxConnection = CobarServer.getInstance().getConfig().getSystem().getMaxConnection();
        return CobarServer.getInstance().getConnectionCount() <= maxConnection;
    }

    @Override
    protected long genConnId() {
        return g.nextId();
    }

    public static long fetchNextConnId() {
        return g.nextId();
    }

    @Override
    public FrontendAuthenticator createFrontendAuthenticator(FrontendConnection conn) {
        if (isPrivilegeMode()) {
            return new FrontendAuthorityAuthenticator(conn);
        } else {
            return new FrontendAuthenticator(conn);
        }
    }

    @Override
    public void initDB(byte[] data) {
        super.initDB(data);
        switchDb(this.schema);
    }

    protected void switchDb(String schema) {
        if (schemaConfig != null && schemaConfig.isDropped()) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("error when close", e);
                } finally {
                    conn = null;
                    releaseLockAndRemoveMdlContext();
                }
            } else {
                releaseLockAndRemoveMdlContext();
            }
        } else {
            TConnection oldConn = this.conn;
            if (null != oldConn) {
                /**
                 * In mysql, its transaction can be commit
                 *  on different database after using 'use xxx_db',
                 * such as:
                 * <pre>
                 *      use d1;
                 *      begin;
                 *      delete from t1;--trx1
                 *      use d2;
                 *      commit;
                 * the transaction trx1 can be commit or rollback after using database d2.
                 * </pre>
                 *
                 * But in PolarDB-X, after use a new database by using `use xxx_db`;
                 * the trx of the last db will be auto rollback and closed.
                 */
                try {
                    oldConn.close();
                    this.conn = null;
                } catch (SQLException e) {
                    logger.warn("error when close trx conn for last db", e);
                } finally {
                    //
                }
            }
        }
        schemaConfig = null;
        tablePrivCache = null;
        schemaPrivCache = null;

        if (ConfigDataMode.isFastMock() && ExecutorContext.getContext(schema) == null) {
            schemaConfig = MockUtil.mockSchema(schema);
            return;
        }

        if (schema != null) {
            // first try cluster and apploader,then mock
            schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);

            // -- load new created db   --
            if (schemaConfig == null) {

                try {
                    CobarServer.getInstance().getConfig().getClusterLoader().getAppLoader().loadApp(schema);
                } catch (Throwable ex) {
                    if (ex.getMessage().contains("Unknown database")) {
                        return;
                    } else {
                        throw ex;
                    }
                }
                schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);
            }
        }

        if (schemaConfig == null) {
            this.stats = MatrixStatistics.EMPTY;
        } else {
            TDataSource ds = schemaConfig.getDataSource();
            this.stats = ds.getStatistics();
            warmUpDb(ds);
        }
    }

    private void warmUpDb(TDataSource ds) {
        if (CobarServer.getInstance().getConfig().getSystem().getEnableLogicalDbWarmmingUp()) {
            Throwable ex = null;
            try {
                ex = TDataSourceInitUtils.initDataSource(ds);
                if (ex != null) {
                    logger.warn("Failed to init schema " + ds.getSchemaName() + " during using db, the cause is " + ex
                        .getMessage(), ex);
                }
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        if (stmtTxIsolation != -1) {
            return stmtTxIsolation;
        }
        return txIsolation;
    }

    private void recoverTxIsolation() {
        if (stmtTxIsolation != -1) {
            stmtTxIsolation = -1;
            setConnTxIsolation(txIsolation);
        }
    }

    private void setConnTxIsolation(int txIsolation) {
        if (this.conn != null) {
            try {
                this.conn.setTransactionIsolation(txIsolation);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public void setStmtTxIsolation(int txIsolation) {
        if (this.beginTransaction) {
            throw new TddlNestableRuntimeException(
                new SQLException("Transaction characteristics can't be changed while a transaction is in progress",
                    "25001", ErrorCode.ER_CANT_CHANGE_TX_ISOLATION, null));
        }
        this.stmtTxIsolation = txIsolation;
        setConnTxIsolation(txIsolation);
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
        if (!ShareReadViewPolicy.supportTxIsolation(txIsolation)) {
            this.shareReadView = ShareReadViewPolicy.OFF;
        }
        setConnTxIsolation(txIsolation);
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int milliseconds) {
        this.socketTimeout = milliseconds;
        if (this.conn != null) {
            try {
                this.conn.setNetworkTimeout(null, milliseconds);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        setAutocommit(autocommit, false);
    }

    public synchronized void setAutocommit(boolean autocommit, boolean isBegin) {

        // 新事务开始，清掉txId, 保留 BEGIN/START TRANSACTION 的 txId
        if (this.autocommit != autocommit && !isBegin) {
            this.txId = null;
        }

        // 自动提交, 清理事务参数
        if (autocommit) {
            this.trxPolicy = null;
            this.shareReadView = ShareReadViewPolicy.DEFAULT;
        }

        this.autocommit = autocommit;
        if (this.conn != null) {
            try {
                this.conn.setAutoCommit(autocommit);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public InternalTimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZoneId) {
        if ("SYSTEM".equalsIgnoreCase(timeZoneId)) {
            this.conn.resetTimeZone();
            this.timeZone = this.conn.getTimeZone();
            this.extraServerVariables.put("time_zone", timeZoneId);
        } else {
            InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneId);
            if (timeZone == null) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_UNKNOWN_TZ,
                    timeZoneId);
            }
            this.timeZone = timeZone;
            this.extraServerVariables.put("time_zone", timeZoneId);
            if (this.conn != null) {
                this.conn.setTimeZone(timeZone);
            }
        }
    }

    public long getLastInsertId() {
        if (this.conn != null) {
            return this.conn.getLastInsertId();
        } else {
            return 0L;
        }
    }

    public ServerSession getSession() {
        return session;
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;

        if (this.conn != null) {
            this.conn.setSqlMode(sqlMode);
        }
    }

    @Override
    public boolean setCharsetIndex(int ci) {
        boolean result = super.setCharsetIndex(ci);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(charset);
            }
        }
        return result;
    }

    @Override
    public boolean setCharset(String charset) {
        boolean result = super.setCharset(charset);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(charset);
            }
        }

        return result;
    }

    @Override
    public synchronized void ping() {
        Ping.response(this);
    }

    public void execute(ByteString sql,
                        PreparedStmtCache preparedStmtCache,
                        final List<Pair<Integer, ParameterContext>> params) {
        setPreparedStmt(preparedStmtCache);
        execute(sql, false, true, params);
    }

    public void execute(String sql, boolean hasMore) {
        execute(ByteString.from(sql), hasMore);
    }

    public void execute(ByteString sql, boolean hasMore) {
        execute(sql, hasMore, false, null);
    }

    public void execute(ByteString sql, boolean hasMore, boolean prepare,
                        final List<Pair<Integer, ParameterContext>> params) {

        int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
        sqlSample = sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length()));

        final QueryResultHandler handler;
        if (prepare) {
            // Prepare Execute 的查询结果
            handler = new ServerPreparedResultHandler(hasMore);
        } else {
            handler = new ServerResultHandler(hasMore);
        }

        innerExecute(sql, params, handler, null);
    }

    public PreparedStmtCache savePrepare(Statement stmt, boolean isServerPrepare) {
        PreparedStmtCache preparedStmtCache = new PreparedStmtCache(stmt);
        if (isServerPrepare) {
            smForPrepare.put(stmt.getStmtId(), preparedStmtCache);
        } else {
            smForQuery.put(stmt.getStmtId(), preparedStmtCache);
        }
        return preparedStmtCache;
    }

    public SQLStatement parseSqlTableNode(String sql) throws SQLException {
        prepareConnection();

        /*
         * send back the packet method 1) since the prepare response is complex
         * which contains the column metadata, so the command must send to real
         * db first, and we then hook the response and replace the stmt_id by
         * ours. method 2) directly compose COM_STMT_PREPARE response on tddl5
         * according to rule and schema.
         */
        OptimizerContext.setContext(getTddlConnection().getDs().getConfigHolder().getOptimizerContext());

        /*
         * Must set executorContext & optimizerContext before get
         * sqlParseManager & do ast build.
         */
        List<SQLStatement> sqlStatementList = FastsqlUtils.parseSql(sql);

        if (sqlStatementList.size() > 1) {
            throw new IllegalArgumentException("prepare not support multi-sql");
        }

        return sqlStatementList.get(0);
    }

    private void prepareConnection() throws SQLException {
        if (this.getTddlConnection() != null) {
            return;
        }

        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        // 拿一下链接
        getConnection(schema);
    }

    /**
     * 将权限添加到SQL上下文中
     */
    public void updatePrivilegeContext() {
        String user = getUser();
        String host = getHost();
        String schema = getSchema();

        if (this.getTddlConnection() != null) {
            PrivilegeContext privilegeContext = new PrivilegeContext();
            privilegeContext.setUser(getUser());
            privilegeContext.setHost(getHost());
            privilegeContext.setSchema(getSchema());
            privilegeContext.setPrivileges(privileges);
            privilegeContext.setTrustLogin(isTrustLogin());
            privilegeContext.setManaged(isManaged());
            PolarPrivileges polarPrivileges = (PolarPrivileges) privileges;
            PolarAccountInfo polarUserInfo = polarPrivileges.checkAndGetMatchUser(user, host);
            this.setMatchPolarUserInfo(polarUserInfo);
            privilegeContext.setPolarUserInfo(polarUserInfo);
            privilegeContext.setActiveRoles(activeRoles);
            this.conn.getExecutionContext().setPrivilegeContext(privilegeContext);
        }
    }

    public void genTraceId() {

        IdGenerator traceIdGen = TrxIdGenerator.getInstance().getIdGenerator();

        StringBuilder sb = new StringBuilder();

        if (this.autocommit) {
            this.txId = traceIdGen.nextId();
            this.sqlId = 0L;
            sb.append(Long.toHexString(txId));
        } else {
            if (this.txId == null) {
                this.txId = traceIdGen.nextId();
                this.sqlId = 0L;
            }

            this.sqlId++;
            sb.append(Long.toHexString(txId)).append("-").append(this.sqlId);
        }

        this.phySqlId = 0L;
        this.traceId = sb.toString();
    }

    /**
     * 简单做同步，避免erlang/nodejs等异步驱动的模式，提交多个sql后，等返回结果，容易串包，加同步，保证串行处理
     * 在重新调度(reschedule)的时候也有用到
     */
    public synchronized void innerExecute(ByteString sql, List<Pair<Integer, ParameterContext>> params,
                                          QueryResultHandler handler, LoadDataContext dataContext) {

        ByteString realSql = dataContext != null ? ByteString.from(dataContext.getLoadDataSql()) : sql;
        long statExecCpuNano = 0;
        if (MetricLevel.isSQLMetricEnabled(RuntimeStat.getMetricLevel())) {
            statExecCpuNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        // 针对非事务的请求进行链接中断
        if (!CobarServer.getInstance().isOnline() && isAutocommit()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return;
        }

        if (isClosed()) {
            return;
        }

        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + this.schema + "'");
            return;
        }

        statementExecuting.set(true);

        // process sql mock
        if (sqlMock) {
            processMock(sql);
            statementExecuting.set(false);
            return;
        }

        if (ConfigDataMode.isFastMock()) {
            if (sql.startsWith("drop database ")) {
                String dbName = sql.toString().split(" ")[2];
                MockUtil.destroySchema(dbName);
                handler.sendUpdateResult(1);
                handler.sendPacketEnd(false);
                return;
            }
        }

        setLastSqlStartTime(System.nanoTime());

        final AtomicLong rowCount = new AtomicLong();
        ResultSet rs = null;
        Throwable exception = null;
        TPreparedStatement stmt = null;
        ExecutionContext ec = null;
        ITransaction trx = null;
        boolean needRescheduled = false;
        try {
            getConnection(schema);

            if (conn == null || conn.isClosed()) {
                logger.warn("connection has been closed");
                return;
            }

            CobarServer.getInstance().getServerExecutor().initTraceStats(traceId);
            prepareExecutionContext(conn.getExecutionContext());
            conn.getExecutionContext().setTestMode(EagleeyeTestHintParser.parseHint(sql));

            if (!autocommit) {
                // In non Auto-commit mode, A DDL will commit the
                // current trans obj and create a new one. so we must
                // record the trans obj for checking.
                trx = conn.getTrx();
            }
            stmt = (TPreparedStatement) conn.prepareStatement(sql);

            // 将权限添加到SQL上下文中从prepareExecutionContext拆出来，依赖于conn
            updatePrivilegeContext();
            ec = this.conn.getExecutionContext();
            ec.setLoadDataContext(dataContext);
            beforeExecution();

            // set params to prepare statement if exist
            fillParams(stmt, params);

            // 可能里边会执行多个sql，有多个返回结果
            boolean existMoreResults;

            // 第一个sql操作是query则返回true, 不是或没有返回结果就返回false
            boolean isQuery = stmt.execute();

            // 检查第一个查询结果
            if (!isQuery) {
                int affectRows = stmt.getUpdateCount();
                existMoreResults = affectRows >= 0;
            } else {
                existMoreResults = true;
            }

            while (existMoreResults) {

                if (!isQuery) {
                    int affectRows = stmt.getUpdateCount();
                    handler.sendUpdateResult(affectRows);
                    rowCount.set(affectRows);
                    conn.setFoundRows(0);
                    conn.setAffectedRows(affectRows);
                } else {
                    rs = stmt.getResultSet();
                    handler.sendSelectResult(rs, rowCount);
                    conn.setFoundRows(rowCount.get());
                    conn.setAffectedRows(0);
                }

                // 检查后边是否还有其它的查询结果, 同时会暗中关闭上一个resultSet
                isQuery = stmt.getMoreResults();
                if (!isQuery) {
                    int affectRows = stmt.getUpdateCount();
                    existMoreResults = affectRows >= 0;
                } else {
                    existMoreResults = true;
                }

                if (existMoreResults) {
                    handler.sendPacketEnd(true);
                }
            }
        } catch (Throwable e) {
            if (futureCancelErrorCode != null) {
                exception = new TddlRuntimeException(futureCancelErrorCode, e);
            } else {
                exception = e;
            }
        } finally {
            try {
                if (exception instanceof CclRescheduleException) {
                    ((CclRescheduleException) exception).getRescheduleCallback().apply(this);
                    needRescheduled = true;
                }
                if (rescheduled) {
                    needRescheduled = needRescheduled && ec != null && ec.getCclContext() != null && ec.getCclContext()
                        .isReschedule();
                    if (needRescheduled) {
                        rescheduleParam =
                            RescheduleParam.builder().sql(sql).params(params).handler(handler)
                                .dataContext(dataContext)
                                .build();
                    }
                } else {
                    CclManager.getService().end(ec);
                }
            } catch (Throwable e) {
                logger.error("Failed to call ccl end.", e);
            }

            try {
                if (rs != null) {
                    // 异常需要带入结果集游标，比如用于MPP的任务的早停
                    if (exception != null && rs instanceof TResultSet) {
                        ((TResultSet) rs).close(exception);
                    } else {
                        rs.close();
                    }
                }
            } catch (Throwable e) {
                logger.error("Failed to close ResultSet", e);
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    logger.error("Failed to close TStatement", e);
                }
            }
        }

        if (autocommit) {
            if (exception == null) {
                try {
                    conn.commit();
                    recoverTxIsolation();
                } catch (Throwable ex) {
                    exception = ex;
                }
            } else if (conn != null && conn.getTrx() instanceof ReadOnlyTsoTransaction) {
                try {
                    conn.commit();
                } catch (Throwable ex) {
                    //ignore
                }
            }
        }

        synchronized (mdlContextLock) {
            if (mdlContext != null) {
                if (autocommit && exception != null) {
                    // Release mdl on autocommit transaction with execution err
                    mdlContext.releaseTransactionalLocks(txId);
                }

                if (autocommit && exception != null && null != ec && ec.getTransaction() != null) {
                    // Release mdl on autocommit transaction with execution
                    // err
                    mdlContext.releaseTransactionalLocks(ec.getTransaction().getId());
                }
            }
        }

        try {
            CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
            CobarServer.getInstance().getServerExecutor().waitByTraceId(traceId);
        } catch (Throwable ex) {
            logger.error("Interrupted unexpectedly for " + ec.getTraceId(), ex);
        }

        // For non-autocommit, if the current statement is a DDL, we reset the trxId
        // in case that the next statement reuses the same trxId of DDL
        if (!autocommit && conn != null && conn.isDdlStatement()) {
            this.txId = null;

            // since a DDL implicitly commits the last transaction,
            // we reset the transaction state if it is a begin-commit transaction
            if (this.beginTransaction) {
                this.beginTransaction = false;
                this.setAutocommit(true);
                this.setReadOnly(false);
                this.recoverTxIsolation();
            }
        }

        try {
            conn.tryClose();
            if (isClosed()) {
                conn.close();
            }
        } catch (Throwable e) {
            logger.error("Failed to close TConnection", e);
        }

        try {
            if (conn.getExecutionContext() != null) {
                try {
                    afterExecution(conn.getExecutionContext(), realSql, params, schema,
                        exception != null ? -1 : rowCount.get(),
                        getTrxPolicyForLogging(), statExecCpuNano, exception);
                } catch (Throwable ex) {
                    logger.error("Exception happens after execution", ex);
                }
            }
        } catch (Throwable e) {
            logger.error("After execution error", e);
        }

        statementExecuting.set(false);

        if (needRescheduled) {
            return;
        }
        if (exception != null) {
            try {
                //here must close the connection when rowCount > 0 and exception is not null.
                handler.handleError(exception, realSql, rowCount.get() > 0);
            } catch (Throwable ex) {
                logger.error("Failed to send error message", ex);
            }
        } else {
            handler.sendPacketEnd(false);
        }
    }

    private String getTrxPolicyForLogging() {
        if (conn.getTrxPolicyForLogging() != null) {
            return conn.getTrxPolicyForLogging().toString();
        } else if (trxPolicy != null) {
            return trxPolicy.toString();
        }
        return null;
    }

    public synchronized PreStmtMetaData prepareExecute(PreparedStmtCache preparedStmtCache, List<?> params)
        throws SQLException {
        ExecutionContext ec = null;
        try {
            ByteString sql = preparedStmtCache.getStmt().getRawSql();
            TConnection tddlConnection = this.getTddlConnection();
            if (tddlConnection == null) {
                boolean ret = initTddlConnection();
                if (!ret) {
                    return null;
                }
                tddlConnection = this.getTddlConnection();
                if (tddlConnection == null) {
                    throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR,
                        "prepare refuse connection");
                }
            }
            ec = tddlConnection.getExecutionContext();
            prepareExecutionContext(ec);
            tddlConnection.initExecutionContextForPrepare(sql);
            /**
             * 将权限添加到SQL上下文中 因为prepare不走innerExecute,所以针对prepare需要获得内部的目标sql并检查是否有权限执行，
             * 而且有了prepare的检查就不需要检查execute了。
             */
            updatePrivilegeContext();
            ec.setPrivilegeMode(this.isPrivilegeMode());
            return MetaConverter.getMetaData(preparedStmtCache, params, ec);
        } catch (RuntimeException ex) {
            logger.error(ex);
            throw ex;
        } finally {
            // Make sure memory pool is released after query
            if (ec != null) {
                try {
                    ec.clearAllMemoryPool();
                } catch (Throwable e) {
                    logger.warn("Failed to release memory of current request", e);
                }
            }
        }
    }

    private void prepareExecutionContext(ExecutionContext ec) {
        ec.setClientIp(host);
        ec.setConnId(id);
        ec.setRescheduled(rescheduled);
        ec.setTxId(txId);
        ec.setTraceId(traceId);
        ec.setPhySqlId(phySqlId);
        ec.setSchemaName(schema);
        ec.setClientFoundRows(clientFoundRows());

        buildMDC();

        ec.renewMemoryPoolHolder();
    }

    private boolean clientFoundRows() {
        return (clientFlags & Capabilities.CLIENT_FOUND_ROWS) > 0;
    }

    private void beforeExecution() {

        if (this.conn == null) {
            return;
        }
        ExecutionContext ec = this.conn.getExecutionContext();
        if (ec != null) {
            /* enable profile for stmt */
            ec.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, RuntimeStat.getMetricLevel());

            ec.setOnlyUseTmpTblPool(false);
            /*
             * all the sql from ServerConn must NOT be internal
             * system sql of drds
             */
            ec.setInternalSystemSql(false);
            ec.setUsingPhySqlCache(true);
        }
        ec.setStartTime(System.nanoTime());
        ec.setPrivilegeMode(isPrivilegeMode());
        ec.setMdcConnString(buildMDCCache);
    }

    private void afterExecution(ExecutionContext ec, ByteString sql, List<Pair<Integer, ParameterContext>> params,
                                SchemaConfig schema, long lastAffectedRows, String trxPolicy,
                                long startExecTimeNano,
                                Throwable exception) {
        RelOptCost cost = CBOUtil.getCost(ec);
        WorkloadType workloadType = WorkloadUtil.getWorkloadType(ec);
        ExplainResult explainResult = ec.getExplain();
        if (explainResult != null && !explainResult.explainMode.isAnalyze()) {
            workloadType = WorkloadType.TP;
        }

        //do not profile and log the query when the query will be rescheduled.
        if (exception instanceof CclRescheduleException) {
            return;
        }

        long finishFetchSqlRsNano = System.nanoTime();
        double executeTimeMs = (finishFetchSqlRsNano - this.conn.getLastExecutionBeginNano()) / 1e6;

        Integer baselineId = null;
        Integer planId = null;

        // Do plan evolution
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_SPM)) {
            Pair<Integer, Integer> baselineIdAndPlanId = this.conn.updatePlanManagementInfo(
                this.conn.getLastExecutionBeginUnixTime(), executeTimeMs / 1e3,
                exception);
            if (baselineIdAndPlanId != null) {
                baselineId = baselineIdAndPlanId.getKey();
                planId = baselineIdAndPlanId.getValue();
            }
        }
        // feed back execution info to optimizer and spm.
        this.conn.feedBackPlan(exception, executeTimeMs, params);
        boolean sqlMetricEnabled = ExecUtils.isSQLMetricEnabled(ec);
        RuntimeStatistics runtimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
        LogUtils.QueryMetrics metrics = getQueryMetrics(ec, startExecTimeNano, sqlMetricEnabled, runtimeStat);

        /*
         * Record sql info to sql.log
         */
        boolean slowRecorded = recordSlowSql(user, host, String.valueOf(port), schema, sql, lastAffectedRows);
        LogUtils.recordSql(this, "", sql, params, trxPolicy, lastAffectedRows, finishFetchSqlRsNano, metrics,
            baselineId, planId, workloadType, cost, ec.getExecuteMode(), slowRecorded);

        cclMetrics(ec, metrics);
        statCpu(ec, finishFetchSqlRsNano, sqlMetricEnabled, runtimeStat);
        feedBackWorkload(ec, workloadType, finishFetchSqlRsNano);

        // Misc.
        this.setLastActiveTime(finishFetchSqlRsNano);
        this.sqlSample = null;

        this.conn.newExecutionContext();
    }

    private void statCpu(ExecutionContext ec, long finishFetchSqlRsNano, boolean sqlMetricEnabled,
                         RuntimeStatistics runtimeStat) {
        if (sqlMetricEnabled && runtimeStat != null) {
            try {
                long finishClearEnvAndLogSqlNano = System.nanoTime();
                long clearEnvAndLogSqlTimeCost = finishClearEnvAndLogSqlNano - finishFetchSqlRsNano;
                CpuStat cpuStat = runtimeStat.getSqlWholeStageCpuStat();
                cpuStat.addCpuStatItem(CpuStatAttribute.CpuStatAttr.CLOSE_AND_LOG, clearEnvAndLogSqlTimeCost);
            } catch (Throwable e) {
                logger.warn("Failed to add cpu stat ", e);
            }

            SqlType sqlType = ec.getSqlType();
            if (sqlType != null && !SqlTypeUtils.isShowSqlType(sqlType)) {
                //Don't record the last sql on load data mode.
                this.lastSqlRunTimeStat = runtimeStat;
            }
        }
    }

    private LogUtils.QueryMetrics getQueryMetrics(ExecutionContext ec, long startExecTimeNano, boolean sqlMetricEnabled,
                                                  RuntimeStatistics runtimeStat) {
        // Collect the metrics info after query was executed
        LogUtils.QueryMetrics metrics = new LogUtils.QueryMetrics();
        metrics.hasMultiShards = ec.hasScanWholeTable();
        metrics.hasUnpushedJoin = ec.hasUnpushedJoin();
        metrics.hasTempTable = ec.hasTempTable();
        metrics.optimizedWithReturning = ec.isOptimizedWithReturning();
        if (runtimeStat != null) {
            runtimeStat.setFinishExecution(true);
            if (sqlMetricEnabled) {
                metrics.runTimeStat = runtimeStat;
                runtimeStat.collectThreadCpu(ThreadCpuStatUtil.getThreadCpuTimeNano() - startExecTimeNano);
            }
            metrics.sqlTemplateId = ec.getSqlTemplateId();
        }
        return metrics;
    }

    private void feedBackWorkload(ExecutionContext ec, WorkloadType workloadType, long finishFetchSqlRsNano) {
        if (WorkloadUtil.isApWorkload(workloadType)) {
            getStatistics().apLoad++;
        } else {
            getStatistics().tpLoad++;
        }

        if (ExecUtils.isMppMode(ec)) {
            getStatistics().cluster++;
        } else {
            getStatistics().local++;
        }
        getStatistics().timeCost += (finishFetchSqlRsNano - getLastActiveTime()) / 1000;
        getStatistics().request++;
    }

    private void cclMetrics(ExecutionContext ec, LogUtils.QueryMetrics metrics) {
        CclContext cclContext = ec.getCclContext();
        if (isRescheduled()) {
            metrics.cclMetric =
                new CclMetric(CclMetric.RESCHEDULE, rescheduleTask.getWaitEndTs() - rescheduleTask.getWaitStartTs(),
                    rescheduleTask.getCclRuleInfo().getCclRuleRecord().id, rescheduleTask.isHitCache());
        } else if (cclContext != null) {
            metrics.cclMetric = cclContext.getMetric();
        }

        if (metrics.cclMetric != null) {
            switch (metrics.cclMetric.getType()) {
            case CclMetric.KILLED:
                getStatistics().cclKill++;
                break;
            case CclMetric.RUN:
                getStatistics().cclRun++;
                break;
            case CclMetric.WAIT:
                getStatistics().cclWait++;
                break;
            case CclMetric.RESCHEDULE:
                getStatistics().cclReschedule++;
                getStatistics().cclWait++;
                break;
            case CclMetric.WAIT_K:
                getStatistics().cclWaitKill++;
                break;
            }
        }
    }

    public void setSqlSample(String sqlSample) {
        this.sqlSample = sqlSample;
    }

    /**
     * 记录sql执行信息
     */
    private boolean recordSlowSql(String user, String host, String port, SchemaConfig schema, ByteString sqlBytes,
                                  long affectRow) {
        boolean slowRecorded = false;
        if (!SQLRecorderLogger.slowLogger.isInfoEnabled()) {
            return slowRecorded;
        }

        SQLRecorder sqlRecorder = schema.getDataSource().getRecorder();
        long endTime = System.nanoTime() / 1000_000;
        long startTime = getLastActiveTime() / 1000_000;
        try {
            long time = endTime - startTime;
            long thresold = sqlRecorder.getSlowSqlTime();

            // Use slow sql time of appname level first
            if (conn.getExecutionContext().getExtraCmds().containsKey(ConnectionProperties.SLOW_SQL_TIME)) {
                thresold = conn.getExecutionContext().getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
            }

            if (time > thresold) {
                slowRecorded = true;
                this.getStatistics().slowRequest++;

                String sql;
                long length = sqlRecorder.getMaxSizeThresold();
                if (sqlBytes.length() > length) {
                    sql = sqlBytes.substring(0, (int) length) + "...";
                } else {
                    sql = sqlBytes.toString();
                }

                String formatted = LogFormat.formatLog(sql);
                sqlRecorder.recordSql(formatted, startTime, user, host, port, this.schema, affectRow, endTime, traceId);

                String slowLog = SQLRecorderLogger.slowLogFormat
                    .format(new Object[] {formatted, String.valueOf(time), String.valueOf(affectRow), traceId});
                SQLRecorderLogger.slowLogger.info(slowLog);
            }
        } catch (Throwable e) {
            logger.error("error when record sql", e);
        }
        return slowRecorded;
    }

    /**
     * 提交事务
     */
    public synchronized void commit(boolean hasMore) {
        try {
            this.txId = null;

            if (this.conn != null) {
                conn.commit();
            }

            if (this.beginTransaction) {
                this.beginTransaction = false;
                this.setAutocommit(true);
                this.setReadOnly(false);
                this.recoverTxIsolation();
            }
            ByteBufferHolder buffer = this.allocate();
            PacketOutputProxyFactory.getInstance().createProxy(this, buffer)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        } catch (Exception ex) {
            this.handleError(ErrorCode.ERR_HANDLE_DATA, ex, "commit", false);
        }
    }

    /**
     * 回滚事务
     */
    public synchronized void rollback(boolean hasMore) {
        try {
            innerRollback();

            ByteBufferHolder buffer = this.allocate();
            PacketOutputProxyFactory.getInstance().createProxy(this, buffer)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        } catch (Exception ex) {
            this.handleError(ErrorCode.ERR_HANDLE_DATA, ex, "rollback", false);
        }
    }

    /**
     * Rollback a transaction
     */
    private void innerRollback() throws SQLException {
        this.txId = null;

        if (this.conn != null) {
            conn.rollback();
        }

        if (this.beginTransaction) {
            this.beginTransaction = false;
            this.setAutocommit(true);
            this.setReadOnly(false);
            this.recoverTxIsolation();
        }
    }

    @Override
    public void handleError(int errCode, Throwable t) {
        handleError(errCode, t, null, false);
    }

    public void handleError(int errCode, Throwable t, String sql, boolean fatal) {

        String db = this.schema;
        if (db == null) {
            db = "";
        }
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();

        Throwable ex = t;
        String message = null;
        String sqlState = null;
        int errorCode = errCode;// 输出给用户看的errorCode
        if (errCode == ErrorCode.ERR_HANDLE_DATA) {
            List<Throwable> ths = ExceptionUtils.getThrowableList(t);
            for (int i = ths.size() - 1; i >= 0; i--) {
                Throwable e = ths.get(i);
                if (SQLSyntaxErrorException.class.isInstance(e)) {
                    errorCode = ErrorCode.ER_PARSE_ERROR;
                    ex = e;
                    break;
                } else if (TableNotFoundException.class.isInstance(e)) {
                    errorCode = ErrorCode.ER_NO_SUCH_TABLE;
                    ex = e;
                    break;
                } else if (e instanceof SQLIntegrityConstraintViolationException) {
                    if (schema != null) {
                        schema.getDataSource().getStatistics().integrityConstraintViolationErrorCount++;
                    }
                }
            }

            message = t.getMessage();
            // 获取一下原始异常中带的sql errorcode
            if (t instanceof TddlException) {
                int code = ((TddlException) t).getErrorCode();
                sqlState = ((TddlException) t).getSQLState();
                if (code > 0) {
                    errorCode = ((TddlException) t).getErrorCode();
                }
            } else if (t instanceof TddlNestableRuntimeException) {
                int code = ((TddlNestableRuntimeException) t).getErrorCode();
                sqlState = ((TddlNestableRuntimeException) t).getSQLState();
                if (code > 0) {
                    errorCode = ((TddlNestableRuntimeException) t).getErrorCode();
                }
            }
        } else {
            message = t.getMessage();
        }
        // 根据异常类型和信息，选择日志输出级别。
        if (this.schema == null) {
            // DNS探测日志的schema一定为null,正常应用访问日志schema绝大多数情况下不为null
            if (io_logger.isInfoEnabled()) {
                buildMDC();
                io_logger.info(toString(), t);
            }
        } else {
            if (ex instanceof EOFException || ex instanceof ClosedChannelException) {
                if (logger.isInfoEnabled()) {
                    buildMDC();
                    logger.info(ex);
                }
            } else if (isConnectionReset(ex)) {
                if (logger.isInfoEnabled()) {
                    buildMDC();
                    logger.info(ex);
                }
            } else if (isTableNotFount(ex) || isColumnNotFount(ex)) {
                if (logger.isDebugEnabled()) {
                    buildMDC();
                    logger.debug(ex);
                }
            } else if (isMySQLIntegrityConstraintViolationException(ex)) {
                if (logger.isDebugEnabled()) {
                    buildMDC();
                    logger.debug(ex);
                }
            } else {
                if (logger.isWarnEnabled()) {
                    buildMDC();
                    if (schema != null) {
                        schema.getDataSource().getStatistics().errorCount++;
                    }
                    logger.warn("[ERROR-CODE: " + errCode + "][" + this.traceId + "] SQL: " + sql, ex);
                }
            }
        }

        // Handle deadlock error
        if (null != this.conn && null != this.conn.getTrx() && isDeadLockException(t)) {
            // Prevent this transaction from committing
            this.conn.getTrx().setCrucialError(ERR_TRANS_DEADLOCK);

            // Rollback this trx
            try {
                innerRollback();
            } catch (SQLException exception) {
                logger.warn("rollback failed when deadlock found", exception);
            }
        }

        switch (errCode) {
        case ErrorCode.ERR_HANDLE_DATA:
            writeErrMessage(errorCode, sqlState, message == null ? t.getClass().getSimpleName() : message);
            if (fatal) {
                close();
                return;
            }
            break;
        default:
            close();
        }
    }

    private static boolean isDeadLockException(Throwable t) {
        if (t instanceof TddlNestableRuntimeException && vendorErrorIs((TddlNestableRuntimeException) t,
            SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)) {
            // A local deadlock causes this exception
            return true;
        }

        if (t instanceof TddlRuntimeException && ((TddlRuntimeException) t).getErrorCodeType()
            .equals(ERR_TRANS_DEADLOCK)) {
            // A global/MDL deadlock causes this exception
            return true;
        }

        return false;
    }

    @Override
    protected void closeConfirm() {
        //notify ccl to response this killed query if it is waiting for the rule.
        if (this.conn != null) {
            ExecutionContext executionContext = this.conn.getExecutionContext();
            if (executionContext != null) {
                CclContext cclContext = executionContext.getCclContext();
                if (cclContext != null) {
                    cclContext.getThread().interrupt();
                }
            }
            cancelRescheduleTask(ErrorCode.ERR_HANDLE_DATA, false);
        }
        this.getStatistics().activeConnection.decrementAndGet();
    }

    @Override
    public synchronized boolean reschedule(Function<CclRuleInfo<RescheduleTask>, Boolean> function) {
        final RescheduleTask currentRescheduleTask = this.rescheduleTask;
        if (currentRescheduleTask == null) {
            return false;
        }
        currentRescheduleTask.setWaitEndTs(System.currentTimeMillis());
        boolean active = currentRescheduleTask.getActivation().compareAndSet(false, true);
        if (active) {
            this.executingFuture = processor.getHandler().submit(this.schema, null, () -> {
                try {
                    if (!rescheduled || this.isClosed()) {
                        throw new TddlNestableRuntimeException("can not be rescheduled.");
                    }
                    if (rescheduleParam != null) {
                        int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
                        ByteString sql = rescheduleParam.sql;
                        sqlSample = sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length()));
                        innerExecute(rescheduleParam.sql, rescheduleParam.params, rescheduleParam.handler,
                            rescheduleParam.dataContext);
                    }

                } catch (Throwable e) {
                    handleError(ErrorCode.ERR_HANDLE_DATA, e);
                } finally {
                    if (function != null) {
                        function.apply(currentRescheduleTask.getCclRuleInfo());
                    }
                    setRescheduled(false, null);
                }
            });
        }
        return active;
    }

    @Override
    public boolean isRescheduled() {
        return this.rescheduled;
    }

    @Override
    public void setRescheduled(boolean rescheduled, RescheduleTask rescheduleTask) {
        this.rescheduled = rescheduled;
        this.rescheduleTask = rescheduleTask;
        if (!this.rescheduled) {
            this.rescheduleParam = null;
        }
    }

    @Override
    public void handleRescheduleError(Throwable throwable) {
        try {
            rescheduleParam.handler.handleError(throwable, rescheduleParam.sql, false);
        } catch (Throwable ex) {
            logger.error("Failed to send error message", ex);
        }
    }

    private void cancelRescheduleTask(int errorCode, boolean killQuery) {
        final RescheduleTask currentRescheduleTask = this.rescheduleTask;
        if (currentRescheduleTask != null) {
            boolean active = currentRescheduleTask.getActivation().compareAndSet(false, true);
            if (active) {
                currentRescheduleTask.getCclRuleInfo().getStayCount().decrementAndGet();
                if (killQuery) {
                    handleError(errorCode, new TddlNestableRuntimeException("The query is cancelled."));
                }
                setRescheduled(false, null);
            }
        }
    }

    @Override
    public boolean isCanReschedule() {
        return !isClosed.get();
    }

    @Override
    public RescheduleTask getRescheduleTask() {
        return rescheduleTask;
    }

    public void setPreparedStmt(PreparedStmtCache preparedStmtCache) {
        this.conn.getExecutionContext().setIsExecutingPreparedStmt(true);
        this.conn.getExecutionContext().setPreparedStmtCache(preparedStmtCache);
    }

    public void removePreparedCache(String stmtId) {
        this.smForPrepare.delete(stmtId);
        this.conn.getExecutionContext().clearPreparedStmt();
    }

    public void resetPreparedParams(String stmtId, boolean isServerPrepare) {
        if (isServerPrepare) {
            this.smForPrepare.find(stmtId).getStmt().clearParams();
        } else {
            this.smForQuery.find(stmtId).getStmt().clearParams();
        }
    }

    /**
     * Cancel a query with retry.
     * The killing action should be executed in KillExecutor thread-pool
     * The retry waiting should be submitted to TimerExecutor
     */
    private class CancelQueryTask implements Runnable {

        /**
         * If a user sends a kill query command, or the memory is running out, or a slow sql is killed automatically,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ERR_USER_CANCELED}.
         * If a deadlock occurs and causes this kill,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ERR_TRANS_DEADLOCK}.
         * If an MDL is preempted and causes this kill,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ERR_TRANS_PREEMPTED_BY_DDL}.
         */
        private final com.alibaba.polardbx.common.exception.code.ErrorCode errorCode;
        private final int retried;
        private final int retryLimit;
        // Only kill query with this traceId.
        private final String traceIdToBeKilled;

        public CancelQueryTask(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode,
                               int retried, int retryLimit) {
            this.errorCode = errorCode;
            this.retried = retried;
            this.retryLimit = retryLimit;
            this.traceIdToBeKilled = ServerConnection.this.getTraceId();
        }

        public CancelQueryTask(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode,
                               int retried, int retryLimit, String traceIdToBeKilled) {
            this.errorCode = errorCode;
            this.retried = retried;
            this.retryLimit = retryLimit;
            this.traceIdToBeKilled = traceIdToBeKilled;
        }

        @Override
        public void run() {
            if (!StringUtils.equals(ServerConnection.this.getTraceId(), this.traceIdToBeKilled)) {
                // Maybe the query to be killed has already finishes executing,
                // just return and do not kill the current executing query.
                return;
            }

            if (retried >= retryLimit || !statementExecuting.get()) {
                return;
            }

            try {
                doCancel();
            } catch (Exception e) {
                logger.warn("Error when cancel query: ", e);
            }
            retryWithBackoff();
        }

        private void doCancel() throws SQLException {
            // First, set the futureCancelErrorCode,
            // which will be used for error handling in innerExecute().
            futureCancelErrorCode = this.errorCode;

            // Then, kill the trx in the connection,
            // during which all physical connections are killed.
            // If a physical connection is still executing a statement,
            // innerExecute() is waiting for this statement to be finished.
            // If such a physical connection is killed,
            // it throws an interrupted exception to innerExecute(),
            // and innerExecute() will handle the futureCancelErrorCode set before
            // instead of the interrupted exception.
            if (conn != null) {
                conn.kill();
            }

            Future f = executingFuture;
            if (f != null) {
                // Finally, cancel(interrupt) the thread in which innerExecute() is executing.
                f.cancel(true);
            }
        }

        private void retryWithBackoff() {
            long delay = 5 + (long) this.retried * 10;
            CobarServer.getInstance().getTimerTaskExecutor().schedule(() -> {
                CancelQueryTask task = new CancelQueryTask(this.errorCode, this.retried + 1, retryLimit,
                    traceIdToBeKilled);
                CobarServer.getInstance().getKillExecutor().execute(task);
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    public void cancelQuery(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode) {
        if (loadDataHandler != null) {
            loadDataHandler.close();
            loadDataHandler = null;
        }
        if (this.statementExecuting.get()) {
            CobarServer.getInstance().getKillExecutor().execute(() -> {
                buildMDC();
                CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);

                (new CancelQueryTask(errorCode, 0, 10)).run();
            });
        }
        if (rescheduled) {
            cancelRescheduleTask(ErrorCode.ERR_HANDLE_DATA, true);
        }
    }

    @Override
    public boolean close() {
        if (super.close()) {
            final TConnection conn = this.conn;
            if (loadDataHandler != null) {
                loadDataHandler.close();
                loadDataHandler = null;
            }
            if (this.statementExecuting.get()) {
                if (conn.isDdlStatement()) {
                    logger.warn("Connection Killed By Client While Executing DDL");
                    if (conn.getExecutionContext().getDdlContext() != null) {
                        conn.getExecutionContext().getDdlContext().setClientConnectionResetAsTrue();
                    }
                    return true;
                }
                CobarServer.getInstance().getKillExecutor().execute(() -> {
                    buildMDC();
                    CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
                    try {
                        int retry = 0;
                        do {
                            try {
                                if (conn != null) {
                                    conn.kill();
                                }

                                Future f = executingFuture;
                                if (f != null) {
                                    f.cancel(true);
                                    futureCancelErrorCode = null;
                                }
                            } catch (Exception ex) {
                                logger.warn("error when kill", ex);
                            }

                            try {
                                Thread.sleep(5 + retry * 10);
                            } catch (InterruptedException e) {
                                logger.warn(e);
                            }

                        } while (statementExecuting.get() && ++retry < 10);
                        try {
                            if (conn != null) {
                                conn.close();
                            }
                        } catch (Exception ex) {
                            logger.warn("error when kill close", ex);
                        }

                        if (retry >= 10) {
                            logger.error("KILL Failed, retry: " + retry);
                        } else {
                            logger.warn("Connection Killed");
                        }
                    } finally {
                        releaseLockAndRemoveMdlContext();
                    }
                });

                return true;
            } else {
                if (conn != null) {
                    CobarServer.getInstance().getKillExecutor().execute(() -> {
                        buildMDC();
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            logger.warn("error when close", e);
                        } finally {
                            releaseLockAndRemoveMdlContext();
                        }
                    });
                } else {
                    releaseLockAndRemoveMdlContext();
                }

                return true;
            }
        }
        return false;
    }

    /**
     * release Transactional locks and remove context
     */
    private void releaseLockAndRemoveMdlContext() {
        synchronized (mdlContextLock) {
            // remove mdl context
            if (null != this.mdlContext) {
                this.mdlContext.releaseAllTransactionalLocks();
                MdlManager.removeContext(this.mdlContext);
                this.mdlContext = null;
            }
        }
    }

    @Nonnull
    public MatrixStatistics getStatistics() {
        if (this.stats == null) {
            return MatrixStatistics.EMPTY;
        }
        return this.stats;
    }

    public synchronized boolean initOptimizerContext() {
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + getSchema() + "'");
            return false;
        }
        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            ds.init();
        }
        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

        return true;
    }

    private void getConnection(SchemaConfig schema) throws SQLException {
        if (this.isClosed()) {
            return;
        }

        if (this.conn == null || !this.conn.getSchema().equalsIgnoreCase(schema.getName())) { // double-check

            TDataSource ds = schema.getDataSource();
            if (!ds.isInited()) {
                ds.init();
            }

            OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

            TConnection oldConn = this.conn;
            /**
             * oldConn should be null here !
             */
            if (null != oldConn) {
                /**
                 * In mysql, its transaction can be commit
                 *  on different database after using 'use xxx_db',
                 * such as:
                 * <pre>
                 *      use d1;
                 *      begin;
                 *      delete from t1;--trx1
                 *      use d2;
                 *      commit;
                 * the transaction trx1 can be commit or rollback after using database d2.
                 * </pre>
                 *
                 * But in PolarDB-X, after use a new database by using `use xxx_db`;
                 * the trx of the last db will be auto rollback and closed.
                 */
                // Clear trx of old conn
                oldConn.close();
            }

            this.conn = (TConnection) ds.getConnection();
            this.conn.setMdlContext(this.mdlContext);
            this.conn.setServerVariables(this.serverVariables);
            this.conn.setGlobalServerVariables(this.globalServerVariables);
            this.conn.setUserDefVariables(this.userDefVariables);
            this.conn.setExtraServerVariables(this.extraServerVariables);
            this.conn.setConnectionVariables(this.connectionVariables);
            this.conn.setUser(this.getUser() + '@' + this.getHost());
            this.conn.setFrontendConnectionInfo(this.getUser() + '@' + this.getHost() + ':' + this.getPort());
            this.conn.setId(this.getId());
            this.conn.setReadOnly(isReadOnly());
            int txIsolation = getTxIsolation();
            this.conn.setTraceId(this.getTraceId());
            this.conn.setClientFoundRows(clientFoundRows());
            if (txIsolation >= 0) {
                this.conn.setTransactionIsolation(txIsolation);
            }
            if (!autocommit) {
                this.conn.setAutoCommit(autocommit);
            }
            this.conn.setShareReadView(this.shareReadView);

            if (sqlMode != null) {
                this.conn.setSqlMode(sqlMode);
            }

            if (charset != null) {
                this.conn.setEncoding(charset);
            }

            if (this.trxPolicy != null) {
                this.conn.setTrxPolicy(this.trxPolicy);
            }

            // set time zone for session
            if (this.timeZone != null) {
                this.conn.setTimeZone(this.timeZone);
            }
        } else {
            this.conn.setReadOnly(isReadOnly());
        }
    }

    public TConnection getTddlConnection() {
        return this.conn;
    }

    public ITransactionPolicy getTrxPolicy() {
        return this.trxPolicy;
    }

    public void setTrxPolicy(ITransactionPolicy policy) {
        this.trxPolicy = policy;

        if (this.conn != null) {
            this.conn.setTrxPolicy(policy);
        }
    }

    public void setShareReadView(boolean shareReadView) {
        ShareReadViewPolicy.checkTxIsolation(txIsolation);
        ShareReadViewPolicy policy = shareReadView ? ShareReadViewPolicy.ON : ShareReadViewPolicy.OFF;
        if (this.conn != null) {
            this.conn.setShareReadView(policy);
        }
        this.shareReadView = policy;
    }

    public void setBatchInsertPolicy(BatchInsertPolicy policy) {

        if (this.conn != null) {
            this.conn.setBatchInsertPolicy(policy);
        }
    }

    private void fillParams(TPreparedStatement stmt, List<Pair<Integer, ParameterContext>> params) {
        if (params == null) {
            return;
        }

        for (Pair<Integer, ParameterContext> param : params) {
            stmt.setParam(param.getKey(), param.getValue());
        }
    }

    public StatementMap getSmForQuery() {
        return smForQuery;
    }

    public StatementMap getSmForPrepare() {
        return smForPrepare;
    }

    public int getIncStmtId() {
        return stmtId.incrementAndGet();
    }

    @Override
    public void fieldList(byte[] data) {
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        // 取得查询语句
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String tableName = null;

        try {
            tableName = mm.readStringWithNull(CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            return;
        }

        String columnPattern = null;

        try {
            columnPattern = mm.readString(CharsetUtil.getJavaCharset(charset));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            return;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                handleError(ErrorCode.ERR_HANDLE_DATA, e);
            }
        }

        String javaCharset = CharsetUtil.getJavaCharset(charset);
        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        TableMeta table = ds.getConfigHolder().getOptimizerContext().getLatestSchemaManager().getTable(tableName);
        if (table == null) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_TABLE, "Table '" + this.schema + "." + tableName + "' doesn't exit");
            return;
        }
        ByteBufferHolder buffer = this.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(this, buffer);
        proxy.packetBegin();

        for (ColumnMeta cm : table.getAllColumns()) {
            if (columnPattern != null) {
                Object value = new Like(Arrays.asList(new StringType(), new StringType()), null)
                    .compute(new Object[] {cm.getName(), columnPattern}, null);
                boolean res = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(value));
                if (!res) {
                    continue;
                }
            }

            String fieldName = cm.getOriginColumnName();
            if (fieldName != null && fieldName.toLowerCase().equals(IMPLICIT_COL_NAME)) {
                continue;
            }

            FieldPacket field = new FieldPacket();
            field.catalog = StringUtil.encode_0("def", javaCharset);
            field.orgName = StringUtil.encode_0(cm.getOriginColumnName(), javaCharset);
            field.name = StringUtil.encode_0(cm.getOriginColumnName(), javaCharset);
            field.orgTable = StringUtil.encode_0(cm.getOriginTableName(), javaCharset);
            field.table = StringUtil.encode_0(cm.getOriginTableName(), javaCharset);
            field.db = StringUtil.encode_0(this.schema, javaCharset);
            field.length = cm.getLength();
            field.flags = toFlag(cm);
            field.decimals = (byte) 0;
            field.charsetIndex = charsetIndex;

            if (cm.getDataType().getSqlType() != DataType.UNDECIDED_SQL_TYPE) {
                field.type = (byte) (MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(cm.getDataType().getSqlType(),
                    field.decimals)) & 0xff);
            } else {
                field.type = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
            }

            field.definition = new byte[] {(byte) 0xfb};
            field.packetId = getNewPacketId();

            proxy = field.write(proxy);
        }

        EOFPacket eof = new EOFPacket();
        eof.packetId = this.getNewPacketId();
        proxy = eof.write(proxy);

        proxy.packetEnd();
    }

    public void begin(boolean readOnly, IsolationLevel level) {
        if (level != null) {
            setStmtTxIsolation(level.getCode());
        }
        this.beginTransaction = true;
        this.setReadOnly(readOnly);
        this.setAutocommit(false, true);
    }

    public void begin() {
        begin(false, null);
    }

    @Override
    protected void addNetInBytes(long bytes) {
        super.addNetInBytes(bytes);
        this.getStatistics().netIn += bytes;
    }

    @Override
    protected void addNetOutBytes(long bytes) {
        super.addNetOutBytes(bytes);
        this.getStatistics().netOut += bytes;
    }

    @Override
    public void addConnectionCount() {
        this.getStatistics().activeConnection.incrementAndGet();
        this.getStatistics().connectionCount.incrementAndGet();
    }

    public AtomicBoolean isStatementExecuting() {
        return statementExecuting;
    }

    @Override
    public synchronized void setSchema(String schema) {
        if (this.isClosed()) {
            return;
        }
        super.setSchema(schema);
        switchDb(this.schema);
    }

    public SchemaConfig getSchemaConfig() {
        if (schemaConfig == null || schemaConfig.isDropped()) {
            switchDb(this.schema);
        }
        return schemaConfig;
    }

    public Long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public void setSqlSelectLimit(Long sqlSelectLimit) {
        this.sqlSelectLimit = sqlSelectLimit;
    }

    /**
     * 如果没有TConnection就初始化一个，参考innerExecute的方法
     */
    synchronized public boolean initTddlConnection() {
        if (!CobarServer.getInstance().isOnline()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return false;
        }

        // 取得SCHEMA
        String db = this.schema;
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        try {
            buildMDC();
            getConnection(schema);
        } catch (Throwable e) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Initialize failed '" + db + "'");
            return false;
        } finally {
            try {
                if (this.conn != null) {
                    this.conn.tryClose();
                }
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        return true;
    }

    @Override
    public synchronized LoadDataHandler prepareLoadInfile(String sql) {

        // 针对非事务的请求进行链接中断
        if (!CobarServer.getInstance().isOnline() && isAutocommit()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return null;
        }

        if (isClosed()) {
            return null;
        }

        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + this.schema + "'");
            return null;
        }

        statementExecuting.set(true);

        try {
            getConnection(schema);
            if (conn == null) {
                logger.warn("connection has been closed");
                return null;
            }

        } catch (Throwable e) {
            logger.error("Initialize LOAD DATA failed!", e);
            writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "Initialize LOAD DATA failed!");
            return null;
        }

        this.loadDataHandler = new ServerLoadDataHandler(this);
        loadDataHandler.open(sql);
        return loadDataHandler;
    }

    @Override
    public void writeErrMessage(byte id, int errno, String sqlState, String msg) {
        StringBuilder sb = new StringBuilder();
        if (this.traceId != null) {
            sb.append("[").append(traceId).append("]");
        }
        sb.append("[").append(CobarServer.getInstance().getServerAddress()).append("]");

        if (schema != null) {
            sb.append("[").append(schema).append("]");
        }
        sb.append(msg);

        super.writeErrMessage(id, errno, sqlState, sb.toString());

    }

    public String getTraceId() {
        return traceId;
    }

    public Long getTxId() {
        return txId;
    }

    public boolean isReadOnly() {
        if (ConfigDataMode.isFastMock()) {
            return false;
        }
        return this.readOnly;
    }

    public void setReadOnly(boolean b) {
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        this.readOnly = b;
    }

    public boolean isCurrentUserReadOnlyAccount() {
        return isReadOnlyAccount(getUser());
    }

    /**
     * 判断给定用户是否为只读账号
     */

    public boolean isReadOnlyAccount(String user) {
        if (user == null) {
            return false;
        }

        return StringUtils.endsWith(user.toUpperCase(), "_RO");
    }

    public boolean isCurrentUserAdministrator() {
        return isAdministrator(getUser());
    }

    /**
     * 判断给定用户是否为管理员账号, 也称为读写账号: 即具有所有权限点和授权选项的用户. 注意: 一个数据库只能有一个管理员账号.
     */
    public boolean isAdministrator(String user) {
        String schema = getSchema();
        if (user == null || schema == null) {
            return false;
        }
        return TStringUtil.equals(user, schema);
    }

    /**
     * 判断给定用户是否为系统账号 老的DRDS权限系统设计中有两个系统账号, 读写账号: 用户名和数据库名一致 只读账号: 数据库名加_RO或_ro后缀
     */
    public boolean isSystemAccount(String user) {
        return isAdministrator(user) || isReadOnlyAccount(user);
    }

    public boolean isCurrentUserSystemAccount() {
        return isSystemAccount(getUser());
    }

    @Override
    public boolean isPrivilegeMode() {
        return true;
    }

    @Override
    public void read() throws IOException {
        if (loadDataHandler != null) {
            if (loadDataHandler.isFull() && loadDataHandler.isBlocked()) {
                //阻塞当前channel
                ((ServerLoadDataHandler) loadDataHandler).disableConnection();
                return;
            }
        }
        super.read();
    }

    public boolean isEnableANSIQuotes() {
        return enableANSIQuotes;
    }

    public void setEnableANSIQuotes(boolean enableANSIQuotes) {
        this.enableANSIQuotes = enableANSIQuotes;
    }

    public String getSqlSample() {
        if (isRescheduled() && rescheduleParam != null && StringUtils.isEmpty(sqlSample)) {
            ByteString sql = rescheduleParam.sql;
            int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
            sqlSample = sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length()));
        }
        return sqlSample;
    }

    public Object getServerVariable(String key) {
        return serverVariables.get(key);
    }

    public long getLastSqlStartTime() {
        return lastSqlStartTime;
    }

    public void setLastSqlStartTime(long lastSqlStartTime) {
        this.lastSqlStartTime = lastSqlStartTime;
    }

    public boolean isSqlMock() {
        return sqlMock;
    }

    public void setSqlMock(boolean sqlMock) {
        this.sqlMock = sqlMock;
    }

    private void processMock(ByteString sql) {
        if (mockExecutor == null) {
            mockExecutor = new MockExecutor();
        }
        String plan = mockExecutor.getPlan(sql);

        ArrayResultCursor result = new ArrayResultCursor("mock");
        result.addColumn("plan", DataTypes.StringType);
        result.initMeta();
        if (!StringUtils.equalsIgnoreCase(sql.toString(), "show warnings")) {
            result.addRow(new Object[] {plan});
        }

        try {
            IPacketOutputProxy proxy =
                ResultSetUtil.resultSetToPacket(new TResultSet(result, null), charset, this, new AtomicLong(0));
            ResultSetUtil.eofToPacket(proxy, this, 2);
        } catch (Exception ex) {
            logger.error("sql mock error", ex);
        }
    }

    public RuntimeStatistics getLastSqlRunTimeStat() {
        return lastSqlRunTimeStat;
    }

    public void setLastSqlRunTimeStat(RuntimeStatistics lastSqlRunTimeStat) {
        this.lastSqlRunTimeStat = lastSqlRunTimeStat;
    }

    public void invalidatePrivCache() {
        this.schemaPrivCache = null;
        this.tablePrivCache = null;
    }

    public void setActiveRoles(ActiveRoles newActiveRoles) {
        Preconditions.checkNotNull(newActiveRoles, "Active roles can't be null!");
        this.activeRoles = newActiveRoles;
    }

    public ConnectionInfo getConnectionInfo() {
        return new ConnectionInfo(getInstanceId(), getUser(), getHost(), getPort(), getSchema(), getTraceId());
    }

    public ActiveRoles getActiveRoles() {
        return activeRoles;
    }

    public QueryResultHandler createResultHandler(boolean hasMore) {
        return new ServerResultHandler(hasMore);
    }

    /**
     * 生成集群内唯一的编号
     *
     * @author Zeratulll 2016年3月17日 下午11:08:53
     * @since 5.0.0
     */
    private static class ClusterAcceptIdGenerator {

        private final Object lock = new Object();
        private long acceptId = 0L;

        private long nextId() {

            int nodeIndex = 0;
            int nodeCount = 0;

            List<NodeInfo> nodes = GmsNodeManager.getInstance().getAllNodes();
            nodes.sort((node1, node2) -> {
                GmsNodeManager.GmsNode gmsNode1 = (GmsNodeManager.GmsNode) node1;
                GmsNodeManager.GmsNode gmsNode2 = (GmsNodeManager.GmsNode) node2;
                if (gmsNode1.instId.equals(gmsNode2.instId)) {
                    return Integer.compare(gmsNode1.uniqueId, gmsNode2.uniqueId);
                }
                return gmsNode1.instId.compareTo(gmsNode2.instId);
            });
            nodeIndex = nodes.indexOf(GmsNodeManager.getInstance().getLocalNode());
            if (nodeIndex == -1) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CONFIG,
                    "local node not found in nodes list");
            }
            nodeCount = nodes.size();

            if (nodeCount == 0) {
                nodeCount = 1;
            }
            synchronized (lock) {
                if (acceptId * nodeCount + nodeIndex >= MAX_VALUE) {
                    acceptId = 0L;
                }
                return (++acceptId) * nodeCount + nodeIndex;
            }
        }
    }

    class ServerResultHandler implements QueryResultHandler {

        private final boolean hasMore;
        IPacketOutputProxy proxy = null;
        private OkPacket ok = null;

        public ServerResultHandler(boolean hasMore) {
            this.hasMore = hasMore;
        }

        @Override
        public void sendUpdateResult(long affectedRows) {
            ok = new OkPacket();
            ok.packetId = getNewPacketId();
            ok.insertId = conn.getReturnedLastInsertId();
            ok.affectedRows = affectedRows;
            ok.serverStatus = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
            if (!autocommit) {
                ok.serverStatus = MySQLPacket.SERVER_STATUS_IN_TRANS;
            }
            ok.warningCount = conn.getWarningCount();
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows) throws Exception {
            proxy = ResultSetUtil.resultSetToPacket(resultSet, charset, ServerConnection.this, outAffectedRows);
        }

        @Override
        public void sendPacketEnd(boolean hasMoreResults) {
            hasMoreResults |= hasMore;

            if (proxy != null) {
                // 写最后的eof包
                int statusFlags = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
                if (!autocommit) {
                    statusFlags = MySQLPacket.SERVER_STATUS_IN_TRANS;
                }
                if (hasMoreResults) {
                    statusFlags |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
                }
                ResultSetUtil.eofToPacket(proxy, ServerConnection.this, statusFlags);
                proxy = null;

            } else if (ok != null) {
                // 为OK包添加SERVER_MORE_RESULTS_EXISTS标记
                if (hasMoreResults) {
                    ok.serverStatus |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
                }
                ok.write(PacketOutputProxyFactory.getInstance().createProxy(ServerConnection.this));
                ok = null;

            } else {
                throw new AssertionError("illegal state");
            }
        }

        @Override
        public void handleError(Throwable ex, ByteString sql, boolean fatal) {
            if (ex instanceof TddlRuntimeException && ((TddlRuntimeException) ex).getErrorCodeType()
                == com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_NO_DB_ERROR) {
                writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return;
            }
            ServerConnection.this.handleError(ErrorCode.ERR_HANDLE_DATA, ex, sql.toString(), fatal);
        }
    }

    private class ServerPreparedResultHandler extends ServerResultHandler {

        public ServerPreparedResultHandler(boolean hasMore) {
            super(hasMore);
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows) throws Exception {
            proxy = BinaryResultSetUtil.resultSetToPacket(resultSet, charset, ServerConnection.this, outAffectedRows);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static class RescheduleParam {
        private ByteString sql;
        private List<Pair<Integer, ParameterContext>> params;
        private QueryResultHandler handler;
        private LoadDataContext dataContext;
    }

}
