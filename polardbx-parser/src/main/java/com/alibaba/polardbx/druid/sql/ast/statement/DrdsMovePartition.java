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

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DrdsMovePartition extends SQLObjectImpl implements SQLAlterTableItem, SQLAlterTableGroupItem {
    private Map<SQLName, List<SQLName>> instPartitions;

    private boolean subPartitionsMoved;

    public Map<SQLName, SQLExpr> getLocalities() {
        return localities;
    }

    public void setLocalities(
        Map<SQLName, SQLExpr> localities) {
        this.localities = localities;
    }

    private Map<SQLName, SQLExpr> localities;

    public Map<SQLName, List<SQLName>> getInstPartitions() {
        return instPartitions;
    }

    public void setInstPartitions(
        Map<SQLName, List<SQLName>> instPartitions) {
        this.instPartitions = instPartitions;
    }

    public boolean isSubPartitionsMoved() {
        return subPartitionsMoved;
    }

    public void setSubPartitionsMoved(boolean subPartitionsMoved) {
        this.subPartitionsMoved = subPartitionsMoved;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}
