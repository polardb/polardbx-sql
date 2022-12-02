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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import org.apache.commons.lang.StringUtils;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class ParamCountVisitor extends MySqlOutputVisitor {
    private int parameterCount;

    public ParamCountVisitor() {
        super(new StringBuilder());
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        super.visit(x);
        String name = x.getName();
//        if (StringUtils.startsWith(name, "@")) {
//            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NEED_IMPLEMENT,
//                "Does not support user/system parameters.");
//        }
        if ("?".equals(name)) {
            parameterCount++;
        }
        return false;
    }

    public int getParameterCount() {
        return parameterCount;
    }
}
