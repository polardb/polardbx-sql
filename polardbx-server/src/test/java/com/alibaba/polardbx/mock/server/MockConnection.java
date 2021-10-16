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

package com.alibaba.polardbx.mock.server;

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.handler.LoadDataHandler;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.nio.channels.SocketChannel;

/**
 * Mock connection
 *
 * @author changyuan.lh 2019年2月27日 上午12:13:54
 * @since 5.0.0
 */
public class MockConnection extends FrontendConnection {

    private static final Logger logger = LoggerFactory.getLogger(MockConnection.class);

    public MockConnection(SocketChannel channel) {
        super(channel);
    }

    @Override
    public void handleError(int errCode, Throwable t) {
        if (logger.isWarnEnabled()) {
            buildMDC();
            logger.warn("ERROR-CODE: " + errCode, t);
        }

        switch (errCode) {
        case ErrorCode.ERR_HANDLE_DATA:
            writeErrMessage(ErrorCode.ER_YES, t.getMessage());
            break;
        default:
            close();
        }
    }

    @Override
    public void fieldList(byte[] data) {
        writeErrMessage(ErrorCode.ER_NOT_SUPPORTED_YET, "Not supported");
    }

    @Override
    public LoadDataHandler prepareLoadInfile(String sql) {
        return null;
    }

    @Override
    public boolean checkConnectionCount() {
        return true;
    }

    @Override
    public void addConnectionCount() {
        // Noop
    }

    @Override
    public boolean isPrivilegeMode() {
        return false;
    }
}
