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

public class InformationSchemaInnodbLocks extends VirtualView {

    public InformationSchemaInnodbLocks(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.INNODB_LOCKS);
    }

    public InformationSchemaInnodbLocks(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("lock_id", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_trx_id", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_mode", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_type", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_table", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_index", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("lock_space", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("lock_page", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("lock_rec", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("lock_data", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}
