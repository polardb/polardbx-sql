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

public class InformationSchemaInnodbSysTablespaces extends VirtualView {

    public InformationSchemaInnodbSysTablespaces(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.INNODB_SYS_TABLESPACES);
    }

    public InformationSchemaInnodbSysTablespaces(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("SPACE", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FLAG", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("FILE_FORMAT", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ROW_FORMAT", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PAGE_SIZE", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("ZIP_PAGE_SIZE", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("SPACE_TYPE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FS_BLOCK_SIZE", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("FILE_SIZE", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ALLOCATED_SIZE", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}
