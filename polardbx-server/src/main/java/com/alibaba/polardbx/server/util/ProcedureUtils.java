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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * @author wcf
 */
public class ProcedureUtils {

    public static String getSchemaName(ServerConnection serverConnection, SQLName procedureName) {
        String schema = serverConnection.getSchema();
        if (procedureName instanceof SQLPropertyExpr) {
            schema = ((SQLPropertyExpr) procedureName).getOwnerName();
        }
        return schema;
    }

    public static String getFullProcedureName(ServerConnection serverConnection, SQLName procedureName) {
        String schema = getSchemaName(serverConnection, procedureName);
        return schema + "." + procedureName.getSimpleName();
    }

    public static long getCursorMemoryLimit(ServerConnection connection) {
        Object value = connection.getConnectionVariables().get(ConnectionProperties.PL_CURSOR_MEMORY_LIMIT);
        if (value == null) {
            return Long.parseLong(ConnectionParams.PL_CURSOR_MEMORY_LIMIT.getDefault());
        } else {
            return Long.parseLong(String.valueOf(value));
        }
    }

    public static long getProcedureMemoryLimit(ServerConnection connection) {
        Object value = connection.getConnectionVariables().get(ConnectionProperties.PL_MEMORY_LIMIT);
        if (value == null) {
            return Long.parseLong(ConnectionParams.PL_MEMORY_LIMIT.getDefault());
        } else {
            return Long.parseLong(String.valueOf(value));
        }
    }

    public static long getPlInternalCacheSize(ServerConnection connection) {
        Object value = connection.getConnectionVariables().get(ConnectionProperties.PL_INTERNAL_CACHE_SIZE);
        if (value == null) {
            return Long.parseLong(ConnectionParams.PL_INTERNAL_CACHE_SIZE.getDefault());
        } else {
            return Long.parseLong(String.valueOf(value));
        }
    }

    public static long getMaxSpDepth(ServerConnection connection) {
        Object value = connection.getConnectionVariables().get(ConnectionProperties.MAX_PL_DEPTH);
        if (value == null) {
            return Long.parseLong(ConnectionParams.MAX_PL_DEPTH.getDefault());
        } else {
            return Long.parseLong(String.valueOf(value));
        }
    }
}
