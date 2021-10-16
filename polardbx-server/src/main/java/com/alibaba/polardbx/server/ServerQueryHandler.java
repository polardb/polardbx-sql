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

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.QueryHandler;
import com.alibaba.polardbx.server.handler.BalanceHandler;
import com.alibaba.polardbx.server.handler.BeginHandler;
import com.alibaba.polardbx.server.handler.ClearHandler;
import com.alibaba.polardbx.server.handler.CollectHandler;
import com.alibaba.polardbx.server.handler.CommentHandler;
import com.alibaba.polardbx.server.handler.DeallocateHandler;
import com.alibaba.polardbx.server.handler.ExecuteHandler;
import com.alibaba.polardbx.server.handler.PrepareHandler;
import com.alibaba.polardbx.server.handler.ResizeHandler;
import com.alibaba.polardbx.server.handler.SelectHandler;
import com.alibaba.polardbx.server.handler.SetHandler;
import com.alibaba.polardbx.server.handler.ShowHandler;
import com.alibaba.polardbx.server.handler.StartHandler;
import com.alibaba.polardbx.server.handler.UseHandler;
import com.alibaba.polardbx.server.handler.privileges.polar.PrivilegeCommandHandlers;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.server.response.KillHandler;
import com.alibaba.polardbx.server.response.PurgeTransHandler;
import com.alibaba.polardbx.server.response.ReloadHandler;
import com.alibaba.polardbx.server.response.ShowHelp;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.optimizer.parse.mysql.ansiquote.MySQLANSIQuoteTransformer;

import java.nio.charset.Charset;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author xianmao.hexm
 */
public class ServerQueryHandler implements QueryHandler {

    private final ServerConnection source;

    public ServerQueryHandler(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void query(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queryRaw(byte[] data, int offset, int length, Charset charset) {

        ByteString sql = new ByteString(data, offset, length, charset);

        // Treat " as an identifier quote character (like the ` quote character)
        // and not as a string quote character.
        // You can still use ` to quote identifiers with this mode enabled. With
        // ANSI_QUOTES enabled,
        // you cannot use double quotation marks to quote literal strings,
        // because it is interpreted as an identifier.
        if (source.isEnableANSIQuotes()) {
            try {
                MySQLANSIQuoteTransformer ansiQuoteTransformer = new MySQLANSIQuoteTransformer(sql);
                sql = ansiQuoteTransformer.getTransformerdSql();
            } catch (SQLSyntaxErrorException e) {
                source.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, e.getMessage());
            }
        }

        ServerConnection c = this.source;

        if (ConfigDataMode.isFastMock() && ExecutorContext.getContext(source.getSchema()) == null) {
            c.switchDb(source.getSchema());
        }
        if (CobarServer.getInstance().getConfig().isLock()) {
            PacketUtil.getLock().write(PacketOutputProxyFactory.getInstance().createProxy(c));
            return;
        }

        // Split multi-statement into single ones
        MultiStatementSplitter splitter = new MultiStatementSplitter(sql);
        List<ByteString> statements = splitter.split();

        // Returns an OK packet for statements with comment only (e.g. "-- example")
        if (statements.isEmpty()) {
            CommentHandler.handle(sql, c);
            return;
        }

        for (int i = 0; i < statements.size(); i++) {
            executeStatement(c, statements.get(i), i < statements.size() - 1);
        }
    }

    private void executeStatement(ServerConnection c, ByteString sql, boolean hasMore) {
        c.genTraceId();

        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            int rs = ServerParse.parse(sql);
            int commandCode = rs & 0xff;
            // table from into outfile 语句转为select * from table into outfile语句
            if (commandCode == ServerParse.TABLE
                && (sql.indexOf("into outfile") != -1 || sql.indexOf("INTO OUTFILE") != -1)) {
                sql = ServerParse.rewriteTableIntoSql(sql);
                rs = ServerParse.parse(sql);
                commandCode = rs & 0xff;
            }
            switch (commandCode) {
            case ServerParse.SET:
                SetHandler.handleV2(sql, c, rs >>> 8, hasMore);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8, hasMore);
                recordSql = false;
                break;
            case ServerParse.CLEAR:
                ClearHandler.handle(sql, c, rs >>> 8, hasMore);
                recordSql = false;
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8, hasMore);
                recordSql = false;
                break;
            case ServerParse.START:
                StartHandler.handle(sql, c, rs >>> 8, hasMore);
                recordSql = false;
                break;
            case ServerParse.BEGIN:
                BeginHandler.handle(sql, c, hasMore);
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, c, rs >>> 8, hasMore);
                break;
            case ServerParse.COMMIT:
                c.commit(hasMore);
                break;
            case ServerParse.KILL:
                KillHandler.response(sql, rs >>> 8, false, c, hasMore);
                break;
            case ServerParse.KILL_QUERY:
                KillHandler.response(sql, rs >>> 8, true, c, hasMore);
                break;
            case ServerParse.ROLLBACK:
                c.rollback(hasMore, true);
                break;
            case ServerParse.PREPARE:
                PrepareHandler.handle(sql, c, hasMore);
                break;
            case ServerParse.EXECUTE:
                ExecuteHandler.handle(sql, c, rs, hasMore);
                break;
            case ServerParse.DEALLOCATE:
                DeallocateHandler.handle(sql, c, hasMore);
                break;
            case ServerParse.HELP:
                ShowHelp.execute(c);
                break;
            case ServerParse.GRANT:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.REVOKE:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.CREATE_USER:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.DROP_USER:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.CREATE_ROLE:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.DROP_ROLE:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.SET_PASSWORD:
                PrivilegeCommandHandlers.handle(commandCode, c, sql, hasMore);
                break;
            case ServerParse.PURGE_TRANS:
                new PurgeTransHandler(sql.toString(), rs >>> 8, c).execute();
                break;
            case ServerParse.BALANCE:
                BalanceHandler.handle(sql, c);
                recordSql = false;
                break;
            case ServerParse.COLLECT:
                CollectHandler.handle(sql, c, rs >>> 8);
                recordSql = false;
                break;
            case ServerParse.RELOAD:
                ReloadHandler.handle(sql, c);
                recordSql = false;
                break;
            case ServerParse.LOAD_DATA_INFILE_SQL:
                source.prepareLoadInfile(sql.toString());
                recordSql = false;
                break;
            case ServerParse.RESIZE:
                ResizeHandler.handle(sql, c, rs >>> 8, hasMore);
                recordSql = false;
                break;
            default:
                c.execute(sql, hasMore);
                recordSql = false;
            }

        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, sql, sqlEx);
            }
        }
    }
}
