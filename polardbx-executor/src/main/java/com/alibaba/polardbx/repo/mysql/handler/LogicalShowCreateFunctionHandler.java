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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.pl.procedure.CreateProcedureRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureStatusRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.google.common.base.Splitter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateFunction;
import org.apache.calcite.sql.SqlShowCreateProcedure;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LogicalShowCreateFunctionHandler extends HandlerCommon {
    public LogicalShowCreateFunctionHandler(IRepository repo) {
        super(repo);
    }

    private String getFunctionName(RelNode logicalPlan) {
        SqlShowCreateFunction showFunction = (SqlShowCreateFunction) ((LogicalShow) logicalPlan).getNativeSqlNode();
        return showFunction.getFunctionName().toString();
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        String functionName = getFunctionName(logicalPlan);
        try (Connection connection = MetaDbUtil.getConnection()) {
            ArrayResultCursor result = new ArrayResultCursor("ROUTINES");
            result.addColumn("Function", DataTypes.StringType);
            result.addColumn("sql_mode", DataTypes.StringType);
            result.addColumn("Create Function", DataTypes.StringType);
            result.addColumn("character_set_client", DataTypes.StringType);
            result.addColumn("collation_connection", DataTypes.StringType);
            result.addColumn("Database Collation", DataTypes.StringType);
            result.initMeta();

            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<CreateProcedureRecord> records = accessor.getCreateFunction(functionName);
            if (records.size() > 0) {
                CreateProcedureRecord record = records.get(0);
                String procedure = record.name;
                String sqlMode = record.sqlMode;
                String createProcedure = record.definition;
                String characterSetClient = record.characterSetClient;
                String collationConnection = record.collationConnection;
                String databaseCollation = record.databaseCollation;

                result.addRow(new Object[] {
                    procedure, sqlMode, createProcedure,
                    characterSetClient, collationConnection, databaseCollation});
            } else {
                // throw error when user defined function not found
                result.close(new ArrayList<>());
                throw new TddlRuntimeException(ErrorCode.ERR_UDF_NOT_FOUND, functionName);
            }

            return result;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_EXECUTE, ex);
        }
    }
}
