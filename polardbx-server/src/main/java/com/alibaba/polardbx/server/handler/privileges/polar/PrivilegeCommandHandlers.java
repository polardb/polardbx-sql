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

package com.alibaba.polardbx.server.handler.privileges.polar;

import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDropRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlGrantRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlRevokeRoleStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.server.parser.ServerParse.CREATE_ROLE;
import static com.alibaba.polardbx.server.parser.ServerParse.CREATE_USER;
import static com.alibaba.polardbx.server.parser.ServerParse.DROP_ROLE;
import static com.alibaba.polardbx.server.parser.ServerParse.DROP_USER;
import static com.alibaba.polardbx.server.parser.ServerParse.GRANT;
import static com.alibaba.polardbx.server.parser.ServerParse.REVOKE;
import static com.alibaba.polardbx.server.parser.ServerParse.SET_PASSWORD;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;

/**
 * Entry point for privilege command handlers.
 *
 * @author bairui.lrj
 * @since 5.4.9
 */
public class PrivilegeCommandHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(PrivilegeCommandHandlers.class);
    private static final Map<Integer, Map<Class<? extends SQLStatement>, MethodHandle>>
        HANDLER_REGISTRY = new HashMap<>(16);

    static {
        insertHandlerEntry(GRANT, MySqlGrantRoleStatement.class, PolarGrantRoleHandler.class);
        insertHandlerEntry(GRANT, SQLGrantStatement.class, PolarGrantPrivilegeHandler.class);
        insertHandlerEntry(REVOKE, MySqlRevokeRoleStatement.class, PolarRevokeRoleHandler.class);
        insertHandlerEntry(REVOKE, SQLRevokeStatement.class, PolarRevokePrivilegeHandler.class);
        insertHandlerEntry(CREATE_USER, MySqlCreateUserStatement.class, PolarCreateUserHandler.class);
        insertHandlerEntry(DROP_USER, SQLDropUserStatement.class, PolarDropUserHandler.class);
        insertHandlerEntry(CREATE_ROLE, MySqlCreateRoleStatement.class, PolarCreateRoleHandler.class);
        insertHandlerEntry(DROP_ROLE, MySqlDropRoleStatement.class, PolarDropRoleHandler.class);
        insertHandlerEntry(SET_PASSWORD, SQLSetStatement.class, PolarSetPasswordHandler.class);
    }

    private static <S extends SQLStatement, H extends PrivilegeCommandHandler> void insertHandlerEntry(int commandCode,
                                                                                                       Class<S> sqlStatementClass,
                                                                                                       Class<H> handlerClass) {
        try {
            MethodType constorType = MethodType.methodType(void.class, ByteString.class, ServerConnection.class,
                PolarAccountInfo.class, PolarPrivManager.class, sqlStatementClass);
            MethodHandle constor = MethodHandles.publicLookup()
                .findConstructor(handlerClass, constorType);

            HANDLER_REGISTRY.computeIfAbsent(commandCode, c -> new HashMap<>(8))
                .put(sqlStatementClass, constor);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            LOG.error("Failed to build handlers privilege command handlers.", e);
            throw new TddlRuntimeException(ERR_SERVER, "Internal server error.", e);
        }
    }

    public static void handle(int commandCode, ServerConnection conn, ByteString sql, boolean hasMore) {
        SQLStatement stmt = FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes).get(0);
        PolarAccountInfo granter = PolarHandlerCommon.getMatchGranter(conn);
        PrivilegeCommandHandler handler = createHandler(commandCode, conn, sql, granter, stmt);
        handler.handle(hasMore);
    }

    private static PrivilegeCommandHandler createHandler(int commandCode, ServerConnection conn,
                                                         ByteString sql,
                                                         PolarAccountInfo granter, SQLStatement stmt) {
        Map<Class<? extends SQLStatement>, MethodHandle> stmtToHandlerMap =
            HANDLER_REGISTRY.get(commandCode);
        if (stmtToHandlerMap == null) {
            throw new TddlRuntimeException(ERR_SERVER, "Unrecognized privilege command code: " + commandCode);
        }

        MethodHandle handlerConstor = stmtToHandlerMap.get(stmt.getClass());
        if (handlerConstor == null) {
            throw new TddlRuntimeException(ERR_SERVER,
                "Unrecognized statement type: " + stmt.getClass().getSimpleName());
        }

        try {
            return (PrivilegeCommandHandler) handlerConstor
                .invoke(sql, conn, granter, PolarPrivManager.getInstance(), stmt.getClass().cast(stmt));
        } catch (Throwable e) {
            throw new TddlRuntimeException(ERR_SERVER, "Creating privilege handler!", e);
        }
    }
}
