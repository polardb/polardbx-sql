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

package org.apache.calcite.rel.core;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlCreateView;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlRenameTable;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class DDL extends AbstractRelNode {

    private DDL.Operation operation;

    private SqlNode tableName;
    private SqlNode newTableName;
    private SqlNode likeTableName;
    private boolean isPartition = false;
    public RelNode relNode;
    public SqlNode sqlNode;
    protected RelNode input;

    protected final SqlDdl ddl;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param input Input relational expression
     */
    protected DDL(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits);
        this.input = input;
        this.ddl = null;
    }

    protected DDL(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits);
        this.ddl = ddl;
        this.rowType = rowType;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    public SqlNode getNewTableName() {
        if (sqlNode instanceof SqlRenameTable) {
            return ((SqlRenameTable) sqlNode).getRenamedNode();
        }
        return null;
    }

    public void setNewTableName(SqlNode newTableName) {
        this.newTableName = newTableName;
    }

    public boolean isPartition() {
        return isPartition;
    }

    public void setPartition(boolean partition) {
        isPartition = partition;
    }

    public SqlNode getLikeTableName() {
        return likeTableName;
    }

    public void setLikeTableName(SqlNode likeTableName) {
        this.likeTableName = likeTableName;
    }

    public enum Operation {
        CREATE_VIEW {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.CREATE_VIEW;
            }
        },
        DROP_VIEW {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.DROP_VIEW;
            }
        },
        CREATE_TABLE {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.CREATE_TABLE;
            }
        },
        DROP_TABLE {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.DROP_TABLE;
            }
        },
        CREATE_INDEX {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.CREATE_INDEX;
            }
        },
        DROP_INDEX {
            @Override
            public SqlKind toSqlKind() {
                return SqlKind.DROP_INDEX;
            }
        };

        public abstract SqlKind toSqlKind();

        public static DDL.Operation mapFromSqlKind(SqlKind sqlKind) {
            switch (sqlKind) {
            case CREATE_VIEW:
                return CREATE_VIEW;
            case CREATE_TABLE:
                return CREATE_TABLE;
            case DROP_TABLE:
                return DROP_INDEX;
            case CREATE_INDEX:
                return DROP_INDEX;
            case DROP_INDEX:
                return DROP_INDEX;
            }
            return null;
        }
    }

    public DDL.Operation getOperation() {
        return operation;
    }

    public void setOperation(DDL.Operation operation) {
        this.operation = operation;
    }

    public SqlNode getQuerySelNode() {
        if (sqlNode instanceof SqlCreateTable) {
            return ((SqlCreateTable) sqlNode).getQuery();
        } else if (sqlNode instanceof SqlCreateView) {
            return ((SqlCreateView) sqlNode).getQuery();
        }
        return null;
    }

    @Override
    public RelDataType deriveRowType() {
        if (ddl != null) {
            return rowType;
        } else {
            return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
        }
    }

    public SqlKind kind() {
        if (null != ddl) {
            return this.ddl.getKind();
        } else {
            if (operation != null) {
                return getOperation().toSqlKind();
            } else {
                if (sqlNode != null) {
                    return sqlNode.getKind();
                } else {
                    return null;
                }
            }

        }
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "DDL");
        String sql = TStringUtil.replace(ddl.toSqlString(MysqlSqlDialect.DEFAULT).toString(),
            System.lineSeparator(),
            " ");
        pw.item("sql", sql);
        return pw;
    }

    public SqlDdl getAst() {
        return ddl;
    }

    @Override
    public SqlNodeList getHints() {
        return ddl.getHints();
    }

    @Override
    public RelNode setHints(SqlNodeList hints) {
        ddl.setHints(hints);
        return this;
    }

    public RelNode getInput() {
        return input;
    }

    public SqlNode getSqlNode() {
        return sqlNode;
    }

    public void setSqlNode(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }
}
