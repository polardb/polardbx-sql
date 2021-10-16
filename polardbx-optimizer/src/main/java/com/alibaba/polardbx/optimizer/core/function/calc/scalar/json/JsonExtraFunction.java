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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.json;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.json.JsonPathExprParser;
import com.alibaba.polardbx.optimizer.json.JsonPathExprStatement;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author wuheng.zxy 2016-5-24 上午10:23:42
 * @author arnkore 2017-07-20 10:13:00
 */
public abstract class JsonExtraFunction extends AbstractScalarFunction {
    public JsonExtraFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public enum SearchMode {
        ONE,
        ALL
    }

    /**
     * 解析JSON path expression
     */
    protected JsonPathExprStatement parseJsonPathExpr(String pathExpression) {
        try {
            if (null == pathExpression) {
                return null;
            }
            MySQLLexer lexer = new MySQLLexer(pathExpression);
            JsonPathExprParser parser = new JsonPathExprParser(lexer);
            return parser.parse();
        } catch (SQLSyntaxErrorException e) {
            throw new JsonParserException(e);
        }
    }

    protected SearchMode parseSearchMode(String oneOrAll) {
        try {
            return SearchMode.valueOf(oneOrAll.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TARGET_STMT_UNEXPECTED_PARAM,
                "The oneOrAll argument to json_contains_path may take these values: 'one' or 'all'.");
        }
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        throw new NotSupportException();
    }
}
