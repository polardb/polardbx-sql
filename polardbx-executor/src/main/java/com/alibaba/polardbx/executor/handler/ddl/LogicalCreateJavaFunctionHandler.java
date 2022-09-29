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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.CreateJavaFunctionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.table.UserDefinedJavaFunctionAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.UserDefinedJavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJavaFunction;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateJavaFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalCreateJavaFunctionHandler extends HandlerCommon {

    public LogicalCreateJavaFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalCreateJavaFunction logicalCreateJavaFunction = (LogicalCreateJavaFunction) logicalPlan;
        final SqlCreateJavaFunction sqlCreateJavaFunction =
            (SqlCreateJavaFunction) logicalCreateJavaFunction.getNativeSqlNode();
        final String funcName = sqlCreateJavaFunction.getFuncName().toString();
        final List<String> inputTypes = sqlCreateJavaFunction.getInputTypes();
        final String returnType = sqlCreateJavaFunction.getReturnType();
        final String userImportString =
            sqlCreateJavaFunction.getImportString() == null ? "" : sqlCreateJavaFunction.getImportString();
        final String userJavaCode = sqlCreateJavaFunction.getJavaCode();

        if (UserDefinedJavaFunctionManager.currentUdfNum
            >= UserDefinedJavaFunctionManager.SUPPORT_MAX_REGISTER_UDF_NUM) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Cannot add more java_functions");
        }

        if (funcName.equals("") ||
            inputTypes.isEmpty() ||
            returnType.equals("") ||
            userJavaCode.equals("")) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Create java_function syntax error");
        }

        if (ExtraFunctionManager.constainsFunction(funcName.toUpperCase())
            || UserDefinedJavaFunctionManager.containsFunction(funcName.toUpperCase())) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Function %s already exists", funcName));
        }

        String className = funcName.substring(0, 1).toUpperCase() + funcName.substring(1).toLowerCase();

        String code;
        try {
            code = readTemplateToString()
                .replaceAll("\\$\\{importString}", userImportString)
                .replaceAll("\\$\\{className}", className)
                .replaceAll("\\$\\{funcNameUpper}", funcName.toUpperCase())
                .replaceAll("\\$\\{userJavaCode}", userJavaCode);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Read code template error");
        }

        ClassLoader cl = UserDefinedJavaFunctionManager.compileAndLoadClass(code, className);

        List<DataType> inputDataTypes;
        //for non-input functions
        if (inputTypes.size() == 1 && inputTypes.get(0).equalsIgnoreCase("NULL")) {
            inputDataTypes = new ArrayList<>();
        } else {
            inputDataTypes = inputTypes.stream()
                .map(UserDefinedJavaFunctionManager::computeDataType)
                .collect(Collectors.toList());
        }

        DataType resultDataType = UserDefinedJavaFunctionManager.computeDataType(returnType);

        try (Connection connection = MetaDbUtil.getConnection()) {
            UserDefinedJavaFunctionAccessor.insertFunction(funcName.toLowerCase(), className, code, "Java",
                connection, buildInputTypeString(inputTypes), returnType);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Meta Connection error");
        }

        try {
            SyncManagerHelper.sync(new CreateJavaFunctionSyncAction(funcName));
        } catch (Exception e) {
            UserDefinedJavaFunctionManager.dropFunction(funcName);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Add function failed because of sync error");
        }

        try {
            UserDefinedJavaFunctionManager.addFunction(
                cl.loadClass(String.format("%s.%s", UserDefinedJavaFunctionManager.PACKAGE_NAME, className)),
                inputDataTypes, resultDataType);
            final SqlFunction UserDefinedJavaFunction = new SqlFunction(
                funcName.toUpperCase(),
                SqlKind.OTHER_FUNCTION,
                UserDefinedJavaFunctionManager.computeReturnType(returnType),
                InferTypes.FIRST_KNOWN,
                inputDataTypes.isEmpty() ? OperandTypes.NILADIC : OperandTypes.ONE_OR_MORE,
                SqlFunctionCategory.SYSTEM
            );
            TddlOperatorTable.instance().register(UserDefinedJavaFunction);
            RexUtils.addUnpushableFunction(UserDefinedJavaFunction);
            UserDefinedJavaFunctionManager.currentUdfNum++;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Add function error");
        }

        return new AffectRowCursor(0);
    }

    private String buildInputTypeString(List<String> inputTypes) {
        StringBuilder sb = new StringBuilder();
        int size = inputTypes.size();
        for (int i = 0; i < size - 1; i++) {
            sb.append(inputTypes.get((i)));
            sb.append(",");
        }
        sb.append(inputTypes.get(size - 1));
        return sb.toString();
    }

    private String readTemplateToString() throws Exception {
        final String TEMPLATE_PATH =
            "polardbx-optimizer/src/main/java/com/alibaba/polardbx/optimizer/core/function/calc/scalar/UserDefinedJavaFunctionTemplate.txt";
        File file = new File(TEMPLATE_PATH);
        FileReader reader = new FileReader(file);
        BufferedReader bReader = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String s = "";
        while ((s = bReader.readLine()) != null) {
            sb.append(s + "\n");
        }
        bReader.close();
        return sb.toString();
    }

}
