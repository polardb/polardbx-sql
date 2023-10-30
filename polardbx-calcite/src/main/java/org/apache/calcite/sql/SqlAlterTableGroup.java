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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroup extends SqlDdl {
    /**
     * Creates a SqlDdl.
     */
    private static final SqlOperator OPERATOR = new SqlAlterTableGroupOperator();
    private final List<SqlAlterSpecification> alters;
    private final String sourceSql;
    private boolean alterByTable;
    private boolean alterIndexTg;
    protected SqlNode tblNameOfIndex;
    private Map<Integer, Map<SqlNode, RexNode>> partRexInfoCtxByLevel;

    public SqlAlterTableGroup(SqlParserPos pos, SqlNode tableGroupName,
                              boolean alterByTable,
                              boolean alterIndexTg,
                              SqlNode tblNameOfIndex,
                              List<SqlAlterSpecification> alters,
                              String sourceSql) {
        super(OPERATOR, pos);
        this.name = tableGroupName;
        this.alterByTable = alterByTable;
        this.alterIndexTg = alterIndexTg;
        this.tblNameOfIndex = tblNameOfIndex;
        this.alters = alters;
        this.sourceSql = sourceSql;
        for (SqlAlterSpecification item : alters) {
            if (item instanceof SqlAlterTableGroupSplitPartition) {
                ((SqlAlterTableGroupSplitPartition) item).setParent(this);
            } else if (item instanceof SqlAlterTableAddPartition) {
                ((SqlAlterTableAddPartition) item).setParent(this);
            } else if (item instanceof SqlAlterTableModifyPartitionValues) {
                ((SqlAlterTableModifyPartitionValues) item).setParent(this);
            } else if (item instanceof SqlAlterTableReorgPartition) {
                ((SqlAlterTableReorgPartition) item).setParent(this);
            }
        }
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public List<SqlAlterSpecification> getAlters() {
        return alters;
    }

    public SqlNode getTableGroupName() {
        return name;
    }

    public Map<Integer, Map<SqlNode, RexNode>> getPartRexInfoCtxByLevel() {
        return partRexInfoCtxByLevel;
    }

    public void setPartRexInfoCtxByLevel(Map<Integer, Map<SqlNode, RexNode>> partRexInfoCtxByLevel) {
        this.partRexInfoCtxByLevel = partRexInfoCtxByLevel;
    }

    public boolean isAlterByTable() {
        return alterByTable;
    }

    public boolean isAlterIndexTg() {
        return alterIndexTg;
    }

    public void setAlterIndexTg(boolean alterIndexTg) {
        this.alterIndexTg = alterIndexTg;
    }

    public SqlNode getTblNameOfIndex() {
        return tblNameOfIndex;
    }

    public void setTblNameOfIndex(SqlNode tblNameOfIndex) {
        this.tblNameOfIndex = tblNameOfIndex;
    }

    public static class SqlAlterTableGroupOperator extends SqlSpecialOperator {

        public SqlAlterTableGroupOperator() {
            super("ALTER_TABLEGROUP", SqlKind.ALTER_TABLEGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_TABLEGROUP_RESULT",
                    0,
                    columnType)));
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        for (SqlAlterSpecification sqlAlterSpecification : alters) {
            if (sqlAlterSpecification instanceof SqlAlterTableGroupSplitPartition ||
                sqlAlterSpecification instanceof SqlAlterTableGroupExtractPartition ||
                sqlAlterSpecification instanceof SqlAlterTableSplitPartitionByHotValue) {
                sqlAlterSpecification.validate(validator, scope);
            }
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }


    public void setAlterByTable(boolean alterByTable) {
        this.alterByTable = alterByTable;
    }

}
