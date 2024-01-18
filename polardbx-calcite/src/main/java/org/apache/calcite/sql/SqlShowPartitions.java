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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/11 下午11:36
 */
public class SqlShowPartitions extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowPartitionsOperator();

    private boolean showIndexPartMeta = false;
    private SqlNode indexName = null;

    public SqlShowPartitions(SqlParserPos pos,
                             List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                             SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit, specialIdentifiers.size() + operands.size() - 1);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_PARTITIONS;
    }

    public boolean isShowIndexPartMeta() {
        return showIndexPartMeta;
    }

    public void setShowIndexPartMeta(boolean showIndexPartMeta) {
        this.showIndexPartMeta = showIndexPartMeta;
    }

    public SqlNode getIndexName() {
        return indexName;
    }

    public void setIndexName(SqlNode indexName) {
        this.indexName = indexName;
    }

    public static class SqlShowPartitionsOperator extends SqlSpecialOperator {

        public SqlShowPartitionsOperator(){
            super("SHOW_PARTITIONS", SqlKind.SHOW_PARTITIONS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("KEYS", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
