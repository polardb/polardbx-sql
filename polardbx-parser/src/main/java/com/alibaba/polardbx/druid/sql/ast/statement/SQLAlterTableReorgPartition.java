/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLAlterTableReorgPartition extends SQLObjectImpl
    implements SQLAlterTableItem, SQLAlterTableGroupItem {

    private final List<SQLName> names = new ArrayList<SQLName>();

    private final List<SQLObject> partitions = new ArrayList<>();

    private boolean isSubPartition = false;

    public List<SQLObject> getPartitions() {
        return partitions;
    }

    public void addPartition(SQLObject partition) {
        if (partition != null) {
            partition.setParent(this);
        }
        this.partitions.add(partition);
    }

    public List<SQLName> getNames() {
        return names;
    }

    public void addName(SQLName name) {
        this.names.add(name);
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public void setSubPartition(boolean subPartition) {
        isSubPartition = subPartition;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, partitions);
        }
        visitor.endVisit(this);
    }
}
