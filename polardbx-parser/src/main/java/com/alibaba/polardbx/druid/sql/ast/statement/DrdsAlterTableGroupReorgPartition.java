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
public class DrdsAlterTableGroupReorgPartition extends SQLObjectImpl implements SQLAlterTableGroupItem {
    private final List<SQLName> oldPartitions  = new ArrayList<SQLName>(4);
    private final List<SQLObject> newPartitions  = new ArrayList<SQLObject>(4);

    public List<SQLName> getOldPartitions() {
        return oldPartitions;
    }

    public List<SQLObject> getNewPartitions() {
        return newPartitions;
    }

    public void addOldPartition(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.oldPartitions.add(x);
    }

    public void addNewPartition(SQLObject x) {
        if (x != null) {
            x.setParent(this);
        }
        this.newPartitions.add(x);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}