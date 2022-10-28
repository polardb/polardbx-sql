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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.server.parser.ServerParse.CALL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;

public class PlCommandHandlers {
    private static final Logger LOG = LoggerFactory.getLogger(PlCommandHandlers.class);
    private static final Map<Integer, PlCommandHandler> HANDLER_REGISTRY = new HashMap<>(16);

    static {
        insertHandlerEntry(CALL, CallHandler.class);
    }

    private static <H extends PlCommandHandler> void insertHandlerEntry(int commandCode, Class<H> handlerClass) {
        try {
            PlCommandHandler handler = handlerClass.newInstance();
            HANDLER_REGISTRY.put(commandCode, handler);
        } catch (IllegalAccessException | InstantiationException e) {
            LOG.error("Failed to build procedure language command handlers.", e);
            throw new TddlRuntimeException(ERR_SERVER, "Internal server error.", e);
        }
    }

    public static void handle(int commandCode, ServerConnection conn, ByteString sql, boolean hasMore) {
        PlCommandHandler handler = getHandler(commandCode);
        handler.handle(sql, conn, hasMore);
    }

    private static PlCommandHandler getHandler(int commandCode) {
        PlCommandHandler handler = HANDLER_REGISTRY.get(commandCode);
        if (handler == null) {
            throw new TddlRuntimeException(ERR_SERVER, "Unrecognized pl command");
        }

        return handler;
    }
}
