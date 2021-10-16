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

package com.alibaba.polardbx.optimizer.parse;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

/**
 * 函数匹配逻辑
 *
 * @author hongxi.chx
 * @create 2017-12-28 21:12
 */
public class TDDLSqlFunction {

    private static final Logger logger = LoggerFactory.getLogger(TDDLSqlFunction.class);

    private Multimap<String,SqlOperator> sqlFunction = LinkedHashMultimap.create();

    private JDBCFunctionClass f;

    private static TDDLSqlFunction tddlSqlFunction = new TDDLSqlFunction();

    public static TDDLSqlFunction getInstance(){
        return tddlSqlFunction;
    }

    public TDDLSqlFunction() {
        for (Field field : TddlOperatorTable.class.getFields()) {
            try {
                if (SqlFunction.class.isAssignableFrom(field.getType())) {
                    SqlFunction op = (SqlFunction) field.get(TddlOperatorTable.instance());
                    if (op != null) {
                        registFunction(op);
                    }
                } else if (
                        SqlOperator.class.isAssignableFrom(field.getType())) {
                    SqlOperator op = (SqlOperator) field.get(TddlOperatorTable.instance());
                    sqlFunction.put(op.getName(),op);
                }
            } catch (IllegalArgumentException | IllegalAccessException e) {
                logger.error("Init parser sql function error",e );
                Util.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        try {
            Class clazz = Class.forName("org.apache.calcite.sql.SqlJdbcFunctionCall$JdbcToInternalLookupTable");
            Field field = clazz.getDeclaredField("INSTANCE");
            field.setAccessible(true);
            Method lookup = clazz.getDeclaredMethod("lookup", new Class[]{String.class});
            lookup.setAccessible(true);
            f = new JDBCFunctionClass(field,lookup);
        } catch (ClassNotFoundException e) {
            logger.error("init parser sql function error",e );
        } catch (NoSuchFieldException e) {
            logger.error("init parser sql function error",e );
        } catch (NoSuchMethodException e) {
            logger.error("init parser sql function error",e );
        }
    }

    private void registFunction(SqlFunction op) {
        if (op.getSyntax() == SqlSyntax.FUNCTION || op.getSyntax() == SqlSyntax.FUNCTION_ID) {
            sqlFunction.put(op.getName(),op);
        }
    }

    public SqlOperator matchFunction(SqlCall sqlCall) {
        SqlOperator operator = sqlCall.getOperator();
        if(!(operator instanceof SqlUnresolvedFunction)){
            return operator;
        }
        List<SqlNode> operandList = sqlCall.getOperandList();
        return matchFunction((SqlUnresolvedFunction)operator,operandList);
    }

    public SqlOperator matchFunction(SqlUnresolvedFunction function,List<SqlNode> operandList) {
        SqlOperator sqlOperator = null;
        Collection<SqlOperator> sqlOperators = sqlFunction.get(function.getName());
        SqlOperator[] sqlOperatorsArray = sqlOperators.toArray(new SqlOperator[0]);
        if (sqlOperatorsArray.length == 1) {
            return sqlOperatorsArray[0];
        } else if (sqlOperatorsArray.length > 1) {
            for (int i = 0; i < sqlOperators.size(); i++) {
                SqlOperator temp = sqlOperatorsArray[i];
                if(temp.getOperandTypeChecker() == null){
                    continue;
                }
                SqlOperandCountRange operandCountRange = temp.getOperandTypeChecker().getOperandCountRange();
                if(operandCountRange == null){
                    sqlOperator = temp;
                    break;
                }
                if(temp.getKind().name().equals(function.getName())){
                    sqlOperator = temp;
                    break;
                }
                int min = operandCountRange.getMin();
                int max = operandCountRange.getMax();
                int realSize = operandList.size();
                if(min == 0 && realSize == 0){
                    sqlOperator = temp;
                    break;
                }
                if (realSize >= min && realSize <= max) {
                    sqlOperator = temp;
                    break;
                }
            }
        }
        if(sqlOperator == null){
            sqlOperator = getJdbcFunctionOperator(function.getName());
        }
        if(sqlOperator == null){
            sqlOperator = function;
        }
        return sqlOperator;
    }

    private SqlOperator getJdbcFunctionOperator(String name){
        try {
            SqlJdbcFunctionCall.SimpleMakeCall simpleMakeCall = (SqlJdbcFunctionCall.SimpleMakeCall)f.m.invoke(f.f.get(null), new Object[]{name});
            if(simpleMakeCall != null ){
                return simpleMakeCall.getOperator();
            }
            return null;
        } catch (InvocationTargetException ignore) {
            logger.warn("invocation target " + name + "：" + ignore.getMessage());
        } catch (IllegalAccessException e) {
            logger.warn("IllegalAccessException target " + name + "：" + e.getMessage());
        }
        return null;
    }

    private static class JDBCFunctionClass{
        Field f;
        Method m;

        public JDBCFunctionClass(Field f, Method m) {
            this.f = f;
            this.m = m;
        }
    }

}
