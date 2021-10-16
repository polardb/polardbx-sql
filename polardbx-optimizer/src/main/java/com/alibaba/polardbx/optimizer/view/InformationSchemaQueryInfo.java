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

package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

/**
 * @author dylan
 */
public class InformationSchemaQueryInfo extends VirtualView {

    public InformationSchemaQueryInfo(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.QUERY_INFO);
    }

    public InformationSchemaQueryInfo(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        //traceId
        columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //db
        columns.add(new RelDataTypeFieldImpl("DB", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //query
        columns.add(new RelDataTypeFieldImpl("COMMAND", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //state
        columns.add(new RelDataTypeFieldImpl("STATE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //task
        columns.add(new RelDataTypeFieldImpl("TASK", 4, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        //worker
        columns.add(new RelDataTypeFieldImpl("COMPUTE_NODE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //error
        columns.add(new RelDataTypeFieldImpl("ERROR", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //failedtask
        columns.add(new RelDataTypeFieldImpl("FAILED_TASK", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //start
        columns.add(new RelDataTypeFieldImpl("START", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        //time
        columns.add(new RelDataTypeFieldImpl("TIME", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}
