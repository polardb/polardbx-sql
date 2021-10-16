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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author lingce.ldm 2018-01-12 15:52
 */
public class ReplaceTableNameWithQuestionMarkVisitor extends ReplaceTableNameWithSomethingVisitor {
    public ReplaceTableNameWithQuestionMarkVisitor(String defaultSchemaName, ExecutionContext ec) {
        super(defaultSchemaName, ec);
    }

    public ReplaceTableNameWithQuestionMarkVisitor(String defaultSchemaName, boolean handleIndexHint,
                                                   ExecutionContext ec) {
        super(defaultSchemaName, handleIndexHint, ec);
    }

    @Override
    protected SqlNode buildSth(SqlNode sqlNode) {
        if (ConfigDataMode.isFastMock()) {
            return sqlNode;
        }
        return new SqlDynamicParam(TABLE_NAME_PARAM_INDEX, SqlParserPos.ZERO, null);
    }

}
