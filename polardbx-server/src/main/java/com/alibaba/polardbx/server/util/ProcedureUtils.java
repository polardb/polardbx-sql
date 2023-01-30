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

import com.alibaba.polardbx.common.properties.ConfigParam;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.Properties;

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

    public static long getVariableValue(ServerConnection connection, ConfigParam var) {
        String varName = var.getName();
        // get session variable
        Object value = connection.getConnectionVariables().get(varName);
        if (value == null) {
            Properties cnProperties =
                MetaDbInstConfigManager.getInstance().getCnVariableConfigMap();
            if (cnProperties.containsKey(varName)) {
                // get global variable
                value = cnProperties.get(varName);
            } else {
                // get default value
                value = var.getDefault();
            }
        }
        return Long.parseLong(String.valueOf(value));
    }
}
