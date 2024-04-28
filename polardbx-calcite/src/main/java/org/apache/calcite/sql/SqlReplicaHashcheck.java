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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author yudong
 * @since 2023/8/22 15:32
 **/
@Getter
public class SqlReplicaHashcheck extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlReplicaHashcheckOperator();

    protected SqlKind sqlKind = SqlKind.REPLICA_HASH_CHECK;
    protected SqlNode from;
    protected SqlNode where;
    protected List<Object> upperBounds;
    protected List<Object> lowerBounds;

    public SqlReplicaHashcheck(SqlParserPos pos, SqlNode from, SqlNode where, List<Object> lowerBounds,
                               List<Object> upperBounds) {
        super(pos);
        this.from = from;
        this.where = where;
        this.upperBounds = upperBounds;
        this.lowerBounds = lowerBounds;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return sqlKind;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REPLICA HASHCHECK * FROM");
        from.unparse(writer, 0, 0);
        if (where != null) {
            writer.keyword("WHERE");
            where.unparse(writer, 0, 0);
        }
    }

    public static class SqlReplicaHashcheckOperator extends SqlSpecialOperator {
        public SqlReplicaHashcheckOperator() {
            super("SQL_REPLICA_HASH_CHECK", SqlKind.REPLICA_HASH_CHECK);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            return typeFactory.createStructType(columns);
        }
    }
}
