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

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * Created by luoyanxin.
 * <p>
 * for list partition add/drop value
 *
 * @author luoyanxin
 */
public class SQLAlterTableModifyPartitionValues extends SQLObjectImpl
    implements SQLAlterTableItem, SQLAlterTableGroupItem {
    boolean isAdd;
    boolean isDrop;
    String algorithm = null;

    /**
     * The values to be added or be dropped
     */
    final SQLPartition sqlPartition;

    boolean isSubPartition;

    public SQLAlterTableModifyPartitionValues(SQLPartition sqlPartition,
                                              boolean isSubPartition) {
        this.sqlPartition = sqlPartition;
        this.isSubPartition = isSubPartition;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public boolean isAdd() {
        return isAdd;
    }

    public void setAdd(boolean add) {
        isAdd = add;
    }

    public boolean isDrop() {
        return isDrop;
    }

    public void setDrop(boolean drop) {
        isDrop = drop;
    }

    public SQLPartition getSqlPartition() {
        return sqlPartition;
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }
}
