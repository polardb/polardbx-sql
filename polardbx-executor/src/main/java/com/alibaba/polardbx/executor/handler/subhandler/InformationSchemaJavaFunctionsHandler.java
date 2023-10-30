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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaJavaFunctions;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InformationSchemaJavaFunctionsHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaJavaFunctionsHandler.class);

    public InformationSchemaJavaFunctionsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaJavaFunctions;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (!executionContext.isSuperUserOrAllPrivileges()) {
            PrivilegeContext pc = executionContext.getPrivilegeContext();
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                "no enough privilege to show java functions", pc.getUser(), pc.getHost());
        }

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement statement = metaDbConn.prepareStatement(
                "SELECT * FROM " + GmsSystemTables.JAVA_FUNCTIONS);
            ResultSet rs = statement.executeQuery();) {
            while (rs.next()) {
                cursor.addRow(new Object[] {
                    rs.getString("FUNCTION_NAME"),
                    rs.getString("CLASS_NAME"),
                    rs.getString("CODE"),
                    rs.getString("CODE_LANGUAGE"),
                    rs.getString("INPUT_TYPES"),
                    rs.getString("RETURN_TYPE"),
                    rs.getBoolean("IS_NO_STATE"),
                    rs.getTimestamp("CREATE_TIME")
                });
            }
        } catch (SQLException ex) {
            logger.error("get information schema java_functions failed!", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }
        return cursor;
    }
}
