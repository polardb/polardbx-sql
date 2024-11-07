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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.eagleeye.EagleeyeHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

public class ReplaceTableNameWithTestTableVisitor extends ReplaceTableNameWithSomethingVisitor {

    private final boolean testMode;

    public ReplaceTableNameWithTestTableVisitor(String defaultSchemaName, boolean testMode, boolean isExplain,
                                                ExecutionContext ec) {
        super(defaultSchemaName, ec);
        this.testMode = testMode;
        this.isExplain = isExplain;
    }

    @Override
    protected SqlNode buildSth(SqlNode sqlNode) {
        if (!testMode) {
            return sqlNode;
        }

        if (!(sqlNode instanceof SqlIdentifier)) {
            return sqlNode;
        }

        if (SqlKind.DDL.contains(sqlKind)) {
            //压测模式下不支持DDL的rename操作,抛出异常
            throw new RuntimeException("Don't support DDL in shallow mode!");
        }
        final SqlIdentifier sqlNode1 = (SqlIdentifier) sqlNode;
        List<String> list = new ArrayList<>();
        for (int i = 0; i < sqlNode1.names.size(); i++) {
            String name = sqlNode1.names.get(i);
            if (i == sqlNode1.names.size() - 1) {
                name = EagleeyeHelper.rebuildTableName(name, testMode);
            }
            list.add(name);
        }
        return new SqlIdentifier(ImmutableList.copyOf(list), SqlParserPos.ZERO);
    }

}
