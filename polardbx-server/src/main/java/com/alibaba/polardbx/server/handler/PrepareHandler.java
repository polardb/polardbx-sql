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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ParamCountVisitor;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * Created by simiao.zw on 2014/7/29.
 */
public final class PrepareHandler {

    public static final byte[] PREPARE_OK = {
        0x1a, 00, 00, // len
        01, 00, // packet number
        00, 00, // rows
        02, 00, // server status
        0, 0, // ??
        0x12, 'S', 't', 'a', 't', 'e', 'm', 'e', 'n', 't', ' ', 'p', 'r', 'e', 'p', 'a', 'r', 'e', 'd'};

    public static final byte[] PREPARE_OK_WITH_MORE = {
        0x1a, 00, 00, // len
        01, 00, // packet number
        00, 00, // rows
        0x0a, 00, // server status
        0, 0, // ??
        0x12, 'S', 't', 'a', 't', 'e', 'm', 'e', 'n', 't', ' ', 'p', 'r', 'e', 'p', 'a', 'r', 'e', 'd'};

    /**
     * Process the statement like prepare stmt1 from 'select * from table_0
     * where id = ?'
     */
    public static boolean handle(ByteString stmt, ServerConnection c, boolean hasMore,
                                 boolean inProcedureCall) {
        try {
            c.checkPreparedStmtCount();
            parseAndSaveStmt(stmt, c);
            if (!inProcedureCall) {
                response(c, hasMore);
            }
        } catch (SQLException e) {
            c.writeErrMessage(e.getErrorCode(), null, e.getMessage());
            return false;
        }
        // prepare 请求不计入QPS
        return true;
    }

    private static void response(ServerConnection c, boolean hasMore) {
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, c.allocate());
        proxy.packetBegin();
        proxy.write(hasMore ? PREPARE_OK_WITH_MORE : PREPARE_OK);
        proxy.packetEnd();
    }

    private static void parseAndSaveStmt(ByteString sql, ServerConnection c) throws SQLException {
        try {
            MySqlPrepareStatement mySqlPrepareStatement = (MySqlPrepareStatement) FastsqlUtils.parseSql(sql).get(0);

            String name = mySqlPrepareStatement.getName().getSimpleName();
            String prepareSql = getPrepareSql(c, mySqlPrepareStatement.getFrom());

            TConnection conn = c.getTddlConnection();
            if (conn != null) {
                ExecutionContext ec = conn.getExecutionContext();
                ec.setPrivilegeMode(c.isPrivilegeMode());
            }

            /**
             * need to do precompile stmt_define to know the params
             * types, so that laterbindValueVisitor execute can create parameters
             */
            SQLStatement sqlStatement = c.parsePrepareSqlTableNode(prepareSql);

            ParamCountVisitor paramCountVisitor = new ParamCountVisitor();
            sqlStatement.accept(paramCountVisitor);

            int parameterCount = paramCountVisitor.getParameterCount();
            Statement mystmt = new Statement(name, ByteString.from(prepareSql));
            // store prepare param count
            mystmt.setPrepareParamCount(parameterCount);
            PreparedStmtCache preparedStmtCache = new PreparedStmtCache(mystmt);
            c.savePrepareStmtCache(preparedStmtCache, false);
        } catch (Exception e) {
            throw new SQLSyntaxErrorException("prepare statement error");
        }
    }

    private static String getPrepareSql(ServerConnection c, SQLExpr from) {
        if (from instanceof SQLCharExpr) {
            return ((SQLCharExpr) from).getText();
        } else if (from instanceof SQLVariantRefExpr && ((SQLVariantRefExpr) from).getName().startsWith("@")) {
            String name = getUserDefVarName(((SQLVariantRefExpr) from).getName()).toLowerCase();
            return DataTypes.StringType.convertFrom(c.getUserDefVariables().get(name));
        }
        throw new RuntimeException(String.format("SQLExpr: %s not support yet", from.toString()));
    }

    private static String getUserDefVarName(String name) {
        if (name.startsWith("@")) {
            return name.substring(1);
        } else {
            return name;
        }
    }
}
