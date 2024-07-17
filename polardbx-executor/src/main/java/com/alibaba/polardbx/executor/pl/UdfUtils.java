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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.TypeKnownScalarFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.util.List;
import java.util.stream.Collectors;

public class UdfUtils {
    public static void registerSqlUdf(String createFunctionStr, boolean canPush) {
        SqlUserDefinedFunction udf = createSqlUdf(createFunctionStr, canPush);
        synchronized (TddlOperatorTable.instance()) {
            // register udf
            Multimap<ReflectiveSqlOperatorTable.Key, SqlOperator> operators =
                HashMultimap.create(TddlOperatorTable.instance().getOperators());
            operators.put(new ReflectiveSqlOperatorTable.Key(udf.getName(), udf.getSyntax()), udf);
            TddlOperatorTable.instance().setOperators(operators);
        }
        // enable type coercion
        SqlStdOperatorTable.instance().enableTypeCoercion(udf);
    }

    public static SqlUserDefinedFunction createSqlUdf(String createFunctionStr, boolean canPush) {
        SQLCreateFunctionStatement
            statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(createFunctionStr).get(0);

        // create scalar function
        List<SQLParameter> inputParams = statement.getParameters();
        List<RelDataType> inputTypes =
            inputParams.stream()
                .map(t -> DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), t.getDataType()))
                .collect(Collectors.toList());
        List<String> inputNames =
            inputParams.stream().map(t -> t.getName().getSimpleName()).collect(Collectors.toList());

        RelDataType returnType =
            DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), statement.getReturnDataType());

        Function function = new TypeKnownScalarFunction(returnType, inputTypes, inputNames);

        // create udf
        String functionName = SQLUtils.getRewriteUdfName(statement.getName());
        return new SqlUserDefinedFunction(new SqlIdentifier(functionName, SqlParserPos.ZERO),
            ReturnTypes.explicit(returnType), InferTypes.explicit(inputTypes),
            new AssignableOperandTypeChecker(inputTypes, inputNames), inputTypes, function, canPush);
    }

    public static void unregisterSqlUdf(String functionName) {
        synchronized (TddlOperatorTable.instance()) {
            Multimap<ReflectiveSqlOperatorTable.Key, SqlOperator> operators =
                HashMultimap.create(TddlOperatorTable.instance().getOperators());
            operators.removeAll(new ReflectiveSqlOperatorTable.Key(functionName, SqlSyntax.FUNCTION));
            TddlOperatorTable.instance().setOperators(operators);
        }
        // disable type coercion
        SqlStdOperatorTable.instance().disableTypeCoercion(functionName, SqlSyntax.FUNCTION);
    }

    public static String removeFuncBody(String createFunctionContent) {
        SQLCreateFunctionStatement
            statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(createFunctionContent).get(0);
        statement.setBlock(new SQLBlockStatement());
        return statement.toString(VisitorFeature.OutputPlOnlyDefinition);
    }

    public static void validateContent(String content) {
        String createFunction = UdfUtils.removeFuncBody(content);
        // validate parser
        SQLCreateFunctionStatement
            statement = (SQLCreateFunctionStatement) FastsqlUtils.parseSql(createFunction).get(0);
        // validate input types
        List<SQLParameter> inputParams = statement.getParameters();
        for (SQLParameter param : inputParams) {
            DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), param.getDataType());
        }
        // validate return types
        DataTypeUtil.createBasicSqlType(TddlRelDataTypeSystemImpl.getInstance(), statement.getReturnDataType());
    }
}
