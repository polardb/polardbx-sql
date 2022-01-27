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

import com.alibaba.polardbx.CobarPrivileges;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.factory.FrontendConnectionFactory;
import com.alibaba.polardbx.net.util.SslHandler;
import com.alibaba.polardbx.server.session.ServerSession;
import com.alibaba.polardbx.ssl.SslConstant;
import com.alibaba.polardbx.ssl.SslContextFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.nio.channels.SocketChannel;

/**
 * @author xianmao.hexm
 */
public class ServerConnectionFactory extends FrontendConnectionFactory {
    private static final Logger logger = LoggerFactory.getLogger(ServerConnectionFactory.class);

    @Override
    protected FrontendConnection getConnection(SocketChannel channel) {
        SystemConfig sys = CobarServer.getInstance().getConfig().getSystem();
        ServerConnection c = new ServerConnection(channel);
        c.setPrivileges(new PolarPrivileges(c));
        c.setQueryHandler(new ServerQueryHandler(c));
        c.setStmtHandler(new ServerPrepareStatementHandler(c));
        c.setTxIsolation(ConfigDataMode.getTxIsolation());
        c.setSession(new ServerSession(c));
        if (sys.isSslEnable()) {
            initSslHandler(c);
        }
        return c;
    }

    private void initSslHandler(ServerConnection c) {
        try {
            SSLContext serverContext = SslContextFactory.getServerContext();
            if (serverContext != null) {
                SSLEngine engine = serverContext.createSSLEngine();
                engine.setEnabledProtocols(SslConstant.enabledProtocols);
                engine.setUseClientMode(false);
                // engine.setEnabledCipherSuites(new
                // String[]{"TLS_RSA_WITH_AES_256_CBC_SHA"});

                SslHandler sslHandler = new SslHandler(engine, c);
                c.setSslHandler(sslHandler);
                logger.info("ssl.connection.open " + c.getHost() + ":" + c.getPort());
            }
        } catch (Throwable e) {
            logger.warn("init ssl context failed , just ignore ", e);
        }
    }

}
