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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.StatementHandler;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MysqlStmtPrepareResponsePacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.packet.StmtClosePacket;
import com.alibaba.polardbx.net.packet.StmtExecutePacket;
import com.alibaba.polardbx.net.packet.StmtLongDataPacket;
import com.alibaba.polardbx.net.packet.StmtPreparePacket;
import com.alibaba.polardbx.net.packet.StmtPrepareReponseHeaderPacket;
import com.alibaba.polardbx.net.packet.StmtResetPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.server.conn.ResultSetCachedObj;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;
import com.alibaba.polardbx.server.handler.SetHandler;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.parse.bean.FieldMetaData;
import com.alibaba.polardbx.optimizer.parse.bean.PreStmtMetaData;
import com.alibaba.polardbx.stats.MatrixStatistics;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Created by simiao.zw on 2014/7/30. Here should generate unique stmt_id by
 * Integer on ServerConnection base
 * Modify by hongxi.chx 2018-04-17
 */
public class ServerPrepareStatementHandler implements StatementHandler {

    private static final Logger logger = LoggerFactory.getLogger(ServerPrepareStatementHandler.class);
    private final ServerConnection source;
    /**
     * params in send_long_data are regarded as blob type
     */
    private static final short LONG_DATA_TYPE = MysqlDefs.FIELD_TYPE_BLOB;

    public ServerPrepareStatementHandler(ServerConnection source) {
        this.source = source;
    }

    /**
     * <pre>
     * the input Statement on packet is like:
     * select id,name from test.table_0 where id = ?
     *
     * since the prepare response is complex which contains the column metadata,
     * so the command
     * must send to real db first, and we then hook the response and replace the
     * stmt_id by ours.
     *
     * No need to add param and sql resource lock since its should only impact one
     * ServerConnection which will
     * send request in sync way.
     *
     * http://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
     * </pre>
     */
    @Override
    public void prepare(byte[] data) {
        ServerConnection c = this.source;
        long lastActiveTime = System.nanoTime();

        c.genTraceId();
        ByteString sql = ByteString.EMPTY;
        String stmtIdStr = "";
        long affectedRow = 0;
        try {
            c.checkPreparedStmtCount();

            StmtPreparePacket spp = new StmtPreparePacket();
            int stmtId = c.getIncStmtId();
            stmtIdStr = Integer.toString(stmtId);
            spp.read(data); // convert into spp format

            String javaCharset = CharsetUtil.getJavaCharset(c.getCharset());
            Charset cs = null;
            try {
                cs = Charset.forName(javaCharset);
            } catch (Exception ex) {
                // do nothing
            }
            if (cs == null) {
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + c.getCharset() + "'");
                return;
            }

            sql = new ByteString(spp.arg, cs);

            boolean ret = source.initOptimizerContext();
            if (!ret) {
                return;
            }
            checkMultiQuery(sql);

            int queryType = ServerParse.parse(sql) & 0xff;
            if (prepareTCL(queryType, stmtId, stmtIdStr)) {
                return;
            }

            Statement stmt = new Statement(stmtIdStr, sql);
            PreparedStmtCache preparedStmtCache = new PreparedStmtCache(stmt);

            PreStmtMetaData metaData = c.getPreparedMetaData(preparedStmtCache, null);
            if (metaData == null) {
                return;
            }

            List<FieldMetaData> fieldMetaDataList = metaData.getFieldMetaDataList();
            int bindCount = metaData.getParamCount();
            stmt.setPrepareParamCount(bindCount);
            // 将table into outfile 转为select * from table into outfile
            if (queryType == ServerParse.TABLE
                && (sql.indexOf("into outfile") != -1 || sql.indexOf("INTO OUTFILE") != -1)) {
                sql = ServerParse.rewriteTableIntoSql(sql);
                queryType = ServerParse.SELECT;
            }
            if (queryType == ServerParse.SET) {
                if (bindCount > 0) { // 暂不支持set xxx=?
                    SQLException t = new SQLException("Prepare does not support sql: " + sql);
                    if (logger.isInfoEnabled()) {
                        logger.info("prepare stmt_id:" + stmtIdStr, t);
                    }
                    throw t;
                }
                stmt.setSetQuery(true);
            }

            /**
             * No need to store column meta info to statement, since resultset will contains
             * them after do executeSql on execute
             */
            MysqlStmtPrepareResponsePacket packet = stmtPrepareResponseToPacket(
                stmtId,
                c.getSchema(),
                fieldMetaDataList,
                stmt.getPrepareParamCount(),
                c.getCharset(), c);

            // send back response to client
            packet.write(PacketOutputProxyFactory.getInstance().createProxy(c, c.allocate()));
            c.savePrepareStmtCache(preparedStmtCache, true);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            c.writeErrMessage(e.getErrorCode(), e.getMessage());
            affectedRow = -1;
        } catch (TddlNestableRuntimeException t) {
            logger.error(t.getMessage());
            c.writeErrMessage(t.getErrorCode(), t.getMessage());
            affectedRow = -1;
        } catch (Exception t) {
            logger.error(t);
            c.writeErrMessage(100, t.getMessage());
            affectedRow = -1;
        } finally {
            long endTimeNano = System.nanoTime();
            c.getStatistics().timeCost += (endTimeNano - lastActiveTime) / 1000;
            c.getStatistics().request++;
            MatrixStatistics.requestAllDB.incrementAndGet();
            LogUtils.recordPreparedSql(c, stmtIdStr, sql, endTimeNano, affectedRow);
        }
    }

    /**
     * MySQL Server does not support multi-query in prepare mode
     * Don't worry. The client implementation (e.g. JDBC Driver) will handle this correctly
     */
    private void checkMultiQuery(ByteString sql) throws SQLException {
        List<ByteString> splitted = null;
        try {
            splitted = new MultiStatementSplitter(sql).split();
        } catch (Exception e) {
            logger.warn(e);
        }
        if (splitted == null || splitted.size() != 1) {
            throw new SQLException("You have an error in your SQL syntax", "42000", ErrorCode.ER_PARSE_ERROR);
        }
    }

    /**
     * <pre>
     * http://dev.mysql.com/doc/internals/en/com-stmt-execute.html
     *
     * BTW: COM_STMT_EXECUTE can only has basic types values in protocol, so that
     * we don't consider the param=now() scenarios
     *
     * use synchronized with send_data_long which no need response and might
     * cause race condition with execute.
     * </pre>
     */
    @Override
    public void execute(byte[] data) {

        ServerConnection c = this.source;
        Lock exeLock = null;
        c.genTraceId();
        try {
            StmtExecutePacket packet = new StmtExecutePacket();
            packet.charset = CharsetUtil.getJavaCharset(c.getCharset());
            // read stmt_id firstly then find num_params
            MySQLMessage msg = packet.readBeforeStmtId(data);

            // convert to string stmt_id firstly.
            String stmtId = String.valueOf(packet.stmt_id);

            PreparedStmtCache preparedStmtCache = c.getSmForPrepare().find(stmtId);
            if (preparedStmtCache == null) {
                // jdbc loadbalance 模式下可能导致prepare-execute不在同一CN节点上
                throw new SQLException("Prepared statement has been freed");
            }
            if (executePreparedTCL(preparedStmtCache)) {
                return;
            }

            Statement stmt = preparedStmtCache.getStmt();
            exeLock = stmt.getExeLock();
            exeLock.lock();

            // read the remaining message
            packet.readAfterStmtId(msg, stmt.getPrepareParamCount(), stmt.getLongDataParams(), stmt.getParamTypes());

            // process parameters and modify by Statement
            stmt.setParamArray(packet.valuesArr);

            List<Pair<Integer, ParameterContext>> params = new ArrayList<>();
            processParameters(stmt, params);// 这里将stmt里的prepare参数转换为优化器所需要的对象格式params

            stmt.clearParams();
            if (stmt.isSetQuery()) {
                SetHandler.handleV2(stmt.getRawSql(), c, -1, false, false);
            } else {
                c.execute(stmt.getRawSql(), preparedStmtCache, params, packet.stmt_id, packet.flags);
            }
        } catch (SQLException e) {
            c.writeErrMessage(e.getErrorCode(), e.getMessage());
        } finally {
            if (exeLock != null) {
                exeLock.unlock();
            }
        }
    }

    /**
     * Prepare事务控制语句
     */
    private boolean prepareTCL(int queryType, int stmtId, String stmtIdStr) throws SQLException {
        PreparedStmtCache preparedStmtCache = null;
        if (queryType == ServerParse.BEGIN) {
            preparedStmtCache = PreparedStmtCache.BEGIN_PREPARE_STMT_CACHE;
        }
        if (queryType == ServerParse.COMMIT) {
            preparedStmtCache = PreparedStmtCache.COMMIT_PREPARE_STMT_CACHE;
        }
        if (queryType == ServerParse.ROLLBACK) {
            preparedStmtCache = PreparedStmtCache.ROLLBACK_PREPARE_STMT_CACHE;
        }
        if (preparedStmtCache != null) {
            ServerConnection c = this.source;
            MysqlStmtPrepareResponsePacket packet = stmtPrepareResponseToPacket(
                stmtId,
                c.getSchema(),
                new ArrayList<>(),
                0,
                c.getCharset(), c);

            // send back response to client
            packet.write(PacketOutputProxyFactory.getInstance().createProxy(c, c.allocate()));
            c.savePrepareStmtCache(stmtIdStr, preparedStmtCache, true);
            return true;
        }

        return false;
    }

    /**
     * 执行事务控制语句
     */
    private boolean executePreparedTCL(PreparedStmtCache preparedStmtCache) {
        ServerConnection c = this.source;
        if (preparedStmtCache == PreparedStmtCache.BEGIN_PREPARE_STMT_CACHE) {
            c.begin();
            PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
            return true;
        }

        if (preparedStmtCache == PreparedStmtCache.COMMIT_PREPARE_STMT_CACHE) {
            c.commit(false);
            return true;
        }

        if (preparedStmtCache == PreparedStmtCache.ROLLBACK_PREPARE_STMT_CACHE) {
            c.rollback(false);
            return true;
        }
        return false;
    }

    /**
     * http://dev.mysql.com/doc/internals/en/com-stmt-close.html
     */
    @Override
    public void close(byte[] data) {
        ServerConnection c = this.source;
        try {
            StmtClosePacket packet = new StmtClosePacket();
            packet.read(data);
            String stmt_id = String.valueOf(packet.stmt_id);
            c.removePreparedCache(stmt_id);
            // Close and remove the cached result set when statement is closed.
            c.closeCacheResultSet(packet.stmt_id);
            if (null != c.getTddlConnection() && null != c.getTddlConnection().getExecutionContext()) {
                c.getTddlConnection().getExecutionContext().setCursorFetchMode(false);
            }
            c.setCursorFetchMode(false);
            // no response for STMT_CLOSE
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }

    /**
     * http://dev.mysql.com/doc/internals/en/com-stmt-reset.html#packet-
     * COM_STMT_RESET
     */
    @Override
    public void reset(byte[] data) {
        ServerConnection c = this.source;
        try {
            StmtResetPacket packet = new StmtResetPacket();
            packet.read(data);
            String stmtId = String.valueOf(packet.stmt_id);

            // only reset value, not param type here
            c.resetPreparedParams(stmtId, true);

            // return OK
            PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        }
    }

    /**
     * http://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html since no
     * response to client, then client will send two packet which might cause
     * race-condition to go into this function so use synchronized to sync them
     * <p>
     * send_long_data is invoked before stmt_execute
     */
    @Override
    public void send_long_data(byte[] data) {
        ServerConnection c = this.source;
        Lock exeLock = null;

        try {
            StmtLongDataPacket packet = new StmtLongDataPacket();
            packet.charset = CharsetUtil.getJavaCharset(c.getCharset());
            MySQLMessage mm = packet.readBeforeParamId(data);

            String stmtId = String.valueOf(packet.stmt_id);
            Statement stmt = c.getSmForPrepare().find(stmtId).getStmt();
            exeLock = stmt.getExeLock();
            exeLock.lock();

            if (stmt.getLongDataParams() == null) {
                stmt.initLongDataParams();
            }
            stmt.setParamType(packet.param_id, LONG_DATA_TYPE);
            packet.readAfterAfterParamId(mm, stmt.getParamType(packet.param_id));
            // should append data if exist
            Object val = stmt.getLongDataParam(packet.param_id);
            if (logger.isDebugEnabled()) {
                StringBuffer sqlInfo = new StringBuffer();
                sqlInfo.append("[execute COM_SEND_LONG_DATA]");
                sqlInfo.append("[stmt_id:").append(stmtId).append("] ");
                sqlInfo.append("[param_id:").append(packet.param_id).append(", ");
                sqlInfo.append("existed value:").append(val).append("]");
                logger.debug(sqlInfo.toString());
            }
            if (val == null) {
                stmt.setLongDataParam(packet.param_id, packet.data);
            } else {
                // append data
                if (val instanceof byte[] && packet.data instanceof byte[]) {
                    byte[] originVal = (byte[]) val;
                    byte[] v2 = (byte[]) packet.data;
                    byte[] resultObj = new byte[originVal.length + v2.length];
                    System.arraycopy(originVal, 0, resultObj, 0, originVal.length);
                    System.arraycopy(v2, 0, resultObj, originVal.length, v2.length);
                    stmt.setLongDataParam(packet.param_id, resultObj);
                } else {
                    c.writeErrMessage(ErrorCode.ER_PARSE_ERROR,
                        "send_long_data got wrong value type, current data is: " + " '" + new String(data) + "'");
                    return;
                }
            }

            // no response
        } catch (Exception e) {
            c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, e.getMessage());
        } finally {
            if (exeLock != null) {
                exeLock.unlock();
            }
        }
    }

    @Override
    public void fetchData(byte[] data) {
        ServerConnection c = this.source;

        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        int stmtId = mm.readInt();
        int numRows = mm.readInt();

        c.fetchData(stmtId, numRows);
    }

    private void processParameters(Statement stmt, List<Pair<Integer, ParameterContext>> params) throws SQLException {
        int prepareParamCount = stmt.getPrepareParamCount();
        if (prepareParamCount > 0) {
            if (stmt.getParams() == null || stmt.getParams().size() < prepareParamCount) {
                throw new SQLException("Parameter is not enough");
            }
        }

        for (int i = 0; i < prepareParamCount; i++) {
            int paramIndex = i + 1;
            /**
             * use exact execute first packet data type to extract incoming data and always
             * use setObject1 to set java object to param context, then the optimizer has
             * the capability to recognize it.
             */
            if (logger.isDebugEnabled()) {
                StringBuffer info = new StringBuffer();
                info.append("[procParams]");
                info.append("[stmt_id:").append(stmt.getStmtId()).append("]");
                info.append("[param_idx:").append(paramIndex).append(", ");
                info.append("value:").append(stmt.getParam(i)).append("]");
                logger.debug(info.toString());
            }
            params.add(new Pair<>(paramIndex,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {paramIndex, stmt.getParam(i)})));
        }
    }

    public static int toFlag(FieldMetaData res) {
        int flags = 0;

        final DataType dataType = res.getType();

        if (!res.isNullable()) {
            flags |= 1;
        }

        if (res.isPrimaryKey()) {
            flags |= 2;
        }

        if (res.isUnSigned()) {
            flags |= 32;
        }

        if (res.isAutoincrement()) {
            flags |= 512;
        }

        if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BinaryType)
            || DataTypeUtil.equalsSemantically(dataType, DataTypes.BlobType)
            || DataTypeUtil.equalsSemantically(dataType, DataTypes.BinaryStringType)) {
            flags |= 128;
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BytesType)) {
            // is binary
            flags |= 128;

            // is blob
            flags |= 16;
        }

        return flags;
    }

    private static MysqlStmtPrepareResponsePacket stmtPrepareResponseToPacket(int stmt_id, String db,
                                                                              List<FieldMetaData> fieldMetaDataList,
                                                                              int paramCounts, String charset,
                                                                              ServerConnection c) throws SQLException {
        /**
         * send COM_STMT_PREPARE response
         * http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
         */
        MysqlStmtPrepareResponsePacket resp = new MysqlStmtPrepareResponsePacket();
        // prepare header before write to buffer
        resp.head = new StmtPrepareReponseHeaderPacket();
        resp.head.statement_id = stmt_id;
        resp.head.num_columns = (short) fieldMetaDataList.size();
        resp.head.num_params = (short) paramCounts;

        // common
        int charsetIndex = 63;

        // prepare params
        resp.paramPackets = new FieldPacket[resp.head.num_params];
        for (int i = 0; i < paramCounts; i++) {
            resp.paramPackets[i] = new FieldPacket();
            resp.paramPackets[i].catalog = StringUtil.encode(FieldPacket.DEFAULT_CATALOG_STR, charset);
            resp.paramPackets[i].db = null;
            resp.paramPackets[i].table = null;
            resp.paramPackets[i].orgTable = null;
            // use pseudo param name
            resp.paramPackets[i].name = StringUtil.encode("?", charset);
            // which mysql driver
            // will not check
            resp.paramPackets[i].orgName = null;
            resp.paramPackets[i].charsetIndex = charsetIndex;
            // max length according to resultSetToPacket
            resp.paramPackets[i].length = 20;
            // since always return string
            resp.paramPackets[i].flags = 0;
            // since always return string
            resp.paramPackets[i].decimals = 0;
            /**
             * Since tddl5 prepare feature not send to db, so that no db prepare
             * response from mysql can be referred, so we just treat all param
             * as STRING, (mysql also send all params as STRING on its prepare
             * response), this will not bring trouble for mysql-connector-java
             * since it uses individual setInt..setString..setObject which can
             * be used to decide the param value type in execute phase, as for
             * mysql-connector-c, it will use mysql_stmt_bind_param to do value
             * bind, and it will set actual value refer to client app like
             * x. mysql-connector-c can use
             * fetch_string_with_conversion to convert string to any type
             */
            // always returns string for prepare
            resp.paramPackets[i].type = MysqlDefs.FIELD_TYPE_STRING;
        }

        // prepare fields
        int i = 0;
        resp.fieldPackets = new FieldPacket[resp.head.num_columns];
        for (FieldMetaData res : fieldMetaDataList) {
            resp.fieldPackets[i] = new FieldPacket();
            resp.fieldPackets[i].db = StringUtil.encode(db, charset);
            resp.fieldPackets[i].catalog = StringUtil.encode(FieldPacket.DEFAULT_CATALOG_STR, charset);
            resp.fieldPackets[i].table = StringUtil.encode(res.getTableName(), charset);
            resp.fieldPackets[i].orgTable = StringUtil.encode(res.getTableOriginName(), charset);
            resp.fieldPackets[i].name = StringUtil.encode(res.getFieldName(), charset);
            resp.fieldPackets[i].orgName = StringUtil.encode(res.getOriginFieldName(), charset);
            resp.fieldPackets[i].charsetIndex = charsetIndex;
            resp.fieldPackets[i].length = res.getFieldLength() != 0 ? res.getFieldLength() : Field.DEFAULT_COLUMN_SIZE;
            resp.fieldPackets[i].flags = toFlag(res);
            resp.fieldPackets[i].decimals = toDecimal(res);
            if (res.getType() != DataTypes.UndecidedType) {
                resp.fieldPackets[i].type = (byte) (MysqlDefs
                    .javaTypeMysql(MysqlDefs.javaTypeDetect(res.getType().getSqlType(), resp.fieldPackets[i].decimals))
                    & 0xff);
            } else {
                // default is string
                resp.fieldPackets[i].type = MysqlDefs.FIELD_TYPE_STRING;
            }

            i++;
        }

        return resp;
    }

    /**
     * Refers to
     * http://dev.mysql.com/doc/internals/en/com-query-response.html#packet
     * -Protocol::ColumnDefinition
     */
    private static byte toDecimal(FieldMetaData res) {
        DataType type = res.getType();
        byte decimal = 0;

        if (DataTypeUtil.isStringType(type)
            || DataTypeUtil.isUnderLongType(type)
            || DataTypeUtil.equalsSemantically(DataTypes.ULongType, type)) {
            decimal = 00;
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DoubleType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.FloatType, type)) {
            decimal = 0x1f;
        } else {
            //0x00 to 0x51 for decimals
            decimal = (byte) res.getScale();
        }
        return decimal;
    }

}
