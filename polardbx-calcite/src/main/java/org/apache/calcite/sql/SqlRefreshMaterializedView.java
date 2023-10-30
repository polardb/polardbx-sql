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

import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlRefreshMaterializedView extends SqlDdl {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("REFRESH MATERIALIZED VIEW", SqlKind.REFRESH_MATERIALIZED_VIEW);

    public SqlRefreshMaterializedView(SqlParserPos pos, SqlIdentifier name) {
        super(OPERATOR, pos);
        this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(null);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.REFRESH_MATERIALIZED_VIEW;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public String getSql() {
        SqlPrettyWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
        unparse(writer, 0, 0);
        return writer.toSqlString().getSql();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.sep("REFRESH");
        writer.keyword("MATERIALIZED VIEW");
        name.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        //do nothing
        return;
    }

}
