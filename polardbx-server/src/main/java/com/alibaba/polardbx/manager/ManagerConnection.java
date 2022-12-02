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

package com.alibaba.polardbx.manager;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.handler.LoadDataHandler;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.druid.pool.GetConnectionTimeoutException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.EOFException;
import java.nio.channels.SocketChannel;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author xianmao.hexm 2011-4-22 下午02:23:55
 */
public final class ManagerConnection extends FrontendConnection {

    private static final Logger logger = LoggerFactory.getLogger(ManagerConnection.class);
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    public ManagerConnection(SocketChannel channel) {
        super(channel);

        instanceId = CobarServer.getInstance().getConfig().getSystem().getInstanceId();
    }

    @Override
    protected long genConnId() {
        return ClusterAcceptIdGenerator.getInstance().nextManageId();
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    @Override
    public void handleData(final byte[] data) {
        CobarServer.getInstance().getManagerExecutor().execute(() -> {
            try {
                handler.handle(data);
            } catch (Throwable e) {
                handleError(ErrorCode.ERR_HANDLE_DATA, e);
            }
        });
    }

    @Override
    public void handleError(int errCode, Throwable t) {
        Throwable ex = t;
        String message = null;
        List<Throwable> ths = ExceptionUtils.getThrowableList(t);
        for (int i = ths.size() - 1; i >= 0; i--) {
            Throwable e = ths.get(i);
            if (GetConnectionTimeoutException.class.isInstance(e)) {
                if (e.getCause() != null) {
                    message = e.getCause().getMessage();
                } else {
                    message = e.getMessage();
                }

                break;
            } else if (SQLSyntaxErrorException.class.isInstance(e)) {
                errCode = ErrorCode.ER_PARSE_ERROR;
                ex = e;
                break;
            }

        }

        if (message == null) {
            message = t.getMessage();
        }

        // 根据异常类型和信息，选择日志输出级别。
        if (ex instanceof EOFException) {
            if (logger.isInfoEnabled()) {
                logger.info(ex);
            }
        } else if (isConnectionReset(ex)) {
            if (logger.isInfoEnabled()) {
                logger.info(ex);
            }
        } else {
            logger.warn(ex);
        }
        switch (errCode) {
        case ErrorCode.ERR_HANDLE_DATA:
            String msg = t.getMessage();
            writeErrMessage(ErrorCode.ER_YES, msg == null ? t.getClass().getSimpleName() : msg);
            break;
        default:
            close();
        }
    }

    @Override
    public LoadDataHandler prepareLoadInfile(String sql) {
        writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "handle load file is not supported in ManagerConnection");
        return null;
    }

    @Override
    public void fieldList(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "field list is not supported in ManagerConnection");

    }

    @Override
    public boolean checkConnectionCount() {
        return true;
    }

    @Override
    public void addConnectionCount() {
    }

    @Override
    public boolean isPrivilegeMode() {
        String clusterName = CobarServer.getInstance().getConfig().getSystem().getClusterName();
        return StringUtils.isEmpty(clusterName);
    }
}
