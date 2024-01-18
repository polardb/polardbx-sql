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

package com.alibaba.polardbx.optimizer.partition.datatype.function.udf;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import java.util.Collection;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class UdfJavaFunctionHelper {

    public static IScalarFunction getUdfPartitionJavaFunctionByName(String udfFuncName) {
        IScalarFunction udfJavaFunc = JavaFunctionManager.getInstance().getJavaFunction(udfFuncName);
        return udfJavaFunc;
    }

    public static UdfJavaFunctionMeta createUdfJavaFunctionMetaByName(String udfFuncName,
                                                                      SqlOperator udfFuncAst) {
        if (!checkIfUdfJavaFunctionExists(udfFuncName)) {
            return null;
        }
        UdfJavaFunctionMeta meta = new UdfJavaFunctionMetaImpl(udfFuncName, udfFuncAst);
        return meta;
    }

    /**
     * Check if the udf java function exists
     */
    public static boolean checkIfUdfJavaFunctionExists(String udfFuncName) {
        IScalarFunction udfJavaFunc = getUdfPartitionJavaFunctionByName(udfFuncName);
        return udfJavaFunc != null;
    }

    public static SqlOperator getUdfFuncOperator(String udfFuncName) {
        ReflectiveSqlOperatorTable.Key sqlOpKey =
            new ReflectiveSqlOperatorTable.Key(udfFuncName.toLowerCase(), SqlSyntax.FUNCTION);
        Collection<SqlOperator> udfSqlFuncAstSet = TddlOperatorTable.instance().getOperators().get(sqlOpKey);
        if (udfSqlFuncAstSet.isEmpty()) {
            return null;
        }
        SqlOperator udfSqlFuncOp = udfSqlFuncAstSet.iterator().next();
        return udfSqlFuncOp;
    }

    public static SqlNode getUdfPartFuncCallAst(String udfFuncName, List<SqlNode> funcOpList) {
        ReflectiveSqlOperatorTable.Key sqlOpKey =
            new ReflectiveSqlOperatorTable.Key(udfFuncName.toLowerCase(), SqlSyntax.FUNCTION);
        Collection<SqlOperator> udfSqlFuncAstSet = TddlOperatorTable.instance().getOperators().get(sqlOpKey);
        if (udfSqlFuncAstSet.isEmpty()) {
            return null;
        }
        SqlOperator udfSqlFuncAst = udfSqlFuncAstSet.iterator().next();
        SqlCall newUdfSqlFuncCallAst = udfSqlFuncAst.createCall(SqlParserPos.ZERO, funcOpList);
        return newUdfSqlFuncCallAst;
    }

}
