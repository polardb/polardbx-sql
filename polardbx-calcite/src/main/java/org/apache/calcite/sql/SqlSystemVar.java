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

package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenmo.cm
 * @date 2018/6/17 上午11:52
 */
public class SqlSystemVar extends SqlLiteral {

    /**
     * Scope: SESSION, GLOBAL or maybe null
     */
    private final VariableScope scope;

    /**
     * Variable name excluding starting "@@", '`' might be included
     */
    private final String name;

    /**
     * Creates a <code>SqlSystemVar</code>.
     */
    public static SqlSystemVar create(VariableScope scope, String name, SqlParserPos pos) {
        return new SqlSystemVar(scope, name, pos);
    }

    SqlSystemVar(VariableScope scope, String name, SqlParserPos pos) {
        super(null, SqlTypeName.NULL, pos);
        this.scope = scope;
        this.name = Preconditions.checkNotNull(name);
    }

    public VariableScope getScope() {
        return scope != null ? scope : VariableScope.SESSION;
    }

    public String getName() {
        return StringUtils.strip(name, "`");
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);

        if (scope != null) {
            switch (scope) {
            case GLOBAL:
                writer.print("@@GLOBAL.");
                break;
            case SESSION:
            default:
                writer.print("@@");
                break;
            }
        }

        writer.print(name.toUpperCase());
        writer.endList(selectFrame);
    }

    @Override
    public SqlLiteral clone(SqlParserPos pos) {
        return new SqlSystemVar(scope, name, pos);
    }
}
