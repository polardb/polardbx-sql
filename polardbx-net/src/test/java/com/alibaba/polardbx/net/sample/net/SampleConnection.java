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

package com.alibaba.polardbx.net.sample.net;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.nio.channels.SocketChannel;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_HANDLE_DATA;

/**
 * @author xianmao.hexm 2011-4-21 上午11:22:57
 */
public class SampleConnection extends FrontendConnection {

    private static final Logger logger = LoggerFactory.getLogger(SampleConnection.class);

    public SampleConnection(SocketChannel channel) {
        super(channel);
    }

    @Override
    public void handleError(ErrorCode errorCode, Throwable t) {
        logger.warn(toString(), t);
        switch (errorCode) {
        case ERR_HANDLE_DATA:
            writeErrMessage(ErrorCode.ER_YES, t.getMessage());
            break;
        default:
            close();
        }
    }

    @Override
    public boolean prepareLoadInfile(String sql) {
        return false;
    }

    @Override
    public void binlogDump(byte[] data) {
    }

    @Override
    public void fieldList(byte[] data) {
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
        return false;
    }

    @Override
    protected long genConnId() {
        return ClusterAcceptIdGenerator.getInstance().nextId();
    }
}
