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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaRoutines;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InformationSchemaRoutinesHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaRoutinesHandler.class);

    public InformationSchemaRoutinesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaRoutines;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement statement = metaDbConn.prepareStatement("SELECT * FROM " + GmsSystemTables.ROUTINES);
            ResultSet rs = statement.executeQuery();) {
            while (rs.next()) {
                cursor.addRow(new Object[] {
                    rs.getString("SPECIFIC_NAME"),
                    rs.getString("ROUTINE_CATALOG"),
                    rs.getString("ROUTINE_SCHEMA"),
                    rs.getString("ROUTINE_NAME"),
                    rs.getString("ROUTINE_TYPE"),
                    rs.getString("DATA_TYPE"),
                    rs.getInt("CHARACTER_MAXIMUM_LENGTH"),
                    rs.getInt("CHARACTER_OCTET_LENGTH"),
                    rs.getLong("NUMERIC_PRECISION"),
                    rs.getInt("NUMERIC_SCALE"),
                    rs.getLong("DATETIME_PRECISION"),
                    rs.getString("CHARACTER_SET_NAME"),
                    rs.getString("COLLATION_NAME"),
                    rs.getString("DTD_IDENTIFIER"),
                    rs.getString("ROUTINE_BODY"),
                    getDefiniton(executionContext, rs.getString("ROUTINE_TYPE"), rs.getString("ROUTINE_DEFINITION")),
                    rs.getString("EXTERNAL_NAME"),
                    rs.getString("EXTERNAL_LANGUAGE"),
                    rs.getString("PARAMETER_STYLE"),
                    rs.getString("IS_DETERMINISTIC"),
                    rs.getString("SQL_DATA_ACCESS"),
                    rs.getString("SQL_PATH"),
                    rs.getString("SECURITY_TYPE"),
                    rs.getTimestamp("CREATED"),
                    rs.getTimestamp("LAST_ALTERED"),
                    rs.getString("SQL_MODE"),
                    rs.getString("ROUTINE_COMMENT"),
                    rs.getString("DEFINER"),
                    rs.getString("CHARACTER_SET_CLIENT"),
                    rs.getString("COLLATION_CONNECTION"),
                    rs.getString("DATABASE_COLLATION")
                });
            }
        } catch (SQLException ex) {
            logger.error("get information schema routines failed!", ex);
        }
        return cursor;
    }

    private String getDefiniton(ExecutionContext executionContext, String type, String content) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ORIGIN_CONTENT_IN_ROUTINES)) {
            return content;
        }
        if (PlConstants.PROCEDURE.equalsIgnoreCase(type)) {
            SQLCreateProcedureStatement statement = (SQLCreateProcedureStatement) FastsqlUtils.parseSql(content,
                SQLParserFeature.IgnoreNameQuotes).get(0);
            content = removeLastCharIfNeed(statement.getBlock().toString());
        } else if (PlConstants.FUNCTION.equalsIgnoreCase(type)) {
            SQLCreateFunctionStatement statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(content,
                SQLParserFeature.IgnoreNameQuotes).get(0);
            content = removeLastCharIfNeed(statement.getBlock().toString());
        }
        return content;
    }

    private String removeLastCharIfNeed(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        int length = str.length();
        if (str.charAt(length - 1) == ';') {
            return str.substring(0, length - 1);
        } else {
            return str;
        }
    }
}
