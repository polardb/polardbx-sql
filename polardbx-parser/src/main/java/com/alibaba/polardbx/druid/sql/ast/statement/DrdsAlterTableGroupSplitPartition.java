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
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DrdsAlterTableGroupSplitPartition extends SQLObjectImpl implements SQLAlterTableGroupItem {
    private final List<SQLObject> partitions  = new ArrayList<SQLObject>(4);
    private SQLExpr atValue;
    private SQLName splitPartitionName;

    public List<SQLObject> getPartitions() {
        return partitions;
    }

    public void addPartition(SQLObject x) {
        if (x != null) {
            x.setParent(this);
        }
        this.partitions.add(x);
    }

    public SQLExpr getAtValue() {
        return atValue;
    }

    public void setAtValue(SQLExpr atValue) {
        this.atValue = atValue;
    }

    public void setSplitPartitionName(SQLName splitPartitionName) {
        this.splitPartitionName = splitPartitionName;
    }

    public SQLName getSplitPartitionName() {
        return splitPartitionName;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, atValue);
            acceptChild(visitor, partitions);
        }
        visitor.endVisit(this);
    }
}
