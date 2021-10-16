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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken;

import java.sql.SQLSyntaxErrorException;
import java.util.Set;

/**
 * @author xianmao.hexm
 */
public final class UseHandler {

    public static void handle(ByteString sql, FrontendConnection c, int offset, boolean hasMore) {
        String useSql;
        try {
            MySQLLexer lexer = new MySQLLexer(sql.toString());
            lexer.nextToken();
            if (MySQLToken.IDENTIFIER != lexer.token()) {
                c.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, "Use statement syntax error");
                return;
            }
            useSql = lexer.getSQL().substring(0, lexer.getCurrentIndex());
            lexer.nextToken();
            if (MySQLToken.PUNC_SEMICOLON == lexer.token()) {
                lexer.nextToken();
                if (MySQLToken.EOF != lexer.token()) {
                    c.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR,
                        "Not support multi statement in use statement case");
                    return;
                }
            } else if (MySQLToken.EOF != lexer.token()) {
                c.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, "Use statement syntax error");
                return;
            }
        } catch (SQLSyntaxErrorException e) {
            c.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, e.getMessage());
            return;
        }
        String schema = useSql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
            if (schema.charAt(0) == '`' && schema.charAt(length - 1) == '`') {
                schema = schema.substring(1, length - 1);
            }
        }

        if (schema != null && ConfigDataMode.isFastMock()) {
            c.setSchema(schema);
            c.updateMDC();
            PacketOutputProxyFactory.getInstance().createProxy(c)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
            return;
        }

        // 检查schema的有效性
        Privileges privileges = c.getPrivileges();
        if (schema == null || !privileges.schemaExists(schema)) {
            c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
            return;
        }
        String user = c.getUser();
        boolean userExists = privileges.userExists(user, c.getHost());
        if (!userExists || !privileges.checkQuarantine(user, c.getHost())) {
            c.writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + c.getUser() + "'@'"
                + c.getHost() + "'");
            return;
        }

        Set<String> schemas;
        if (c.isPrivilegeMode()) {
            schemas = privileges.getUserSchemas(user, c.getHost());
        } else {
            schemas = privileges.getUserSchemas(user);
        }
        if (schemas != null && schemas.contains(schema)) {
            c.setSchema(schema);
            c.updateMDC();
            PacketOutputProxyFactory.getInstance().createProxy(c)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        } else {
            String msg = "Access denied for user '" + c.getUser() + "'@'" + c.getHost() + "' to database '"
                + schema + "'";
            c.writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, msg);
        }
    }
}
