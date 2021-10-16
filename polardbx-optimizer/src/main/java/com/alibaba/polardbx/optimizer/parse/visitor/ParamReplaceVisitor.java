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

/**
 * @author busu
 * date: 2021/3/24 6:31 下午
 */
public class ParamReplaceVisitor extends MySqlOutputVisitor {
    private String replaceValue;

    public ParamReplaceVisitor(Appendable appender, String replaceValue) {
        super(appender);
        this.replaceValue = replaceValue;
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        String name = x.getName();
        if ("?".equals(name)) {
            print0("'");
            print0(replaceValue);
            print0("'");
        }
        return false;
    }

}
