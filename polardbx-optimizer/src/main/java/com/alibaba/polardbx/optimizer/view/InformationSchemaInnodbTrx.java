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

public class InformationSchemaInnodbTrx extends VirtualView {

    public InformationSchemaInnodbTrx(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.INNODB_TRX);
    }

    public InformationSchemaInnodbTrx(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("trx_id", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_state", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_started", 2, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns
            .add(new RelDataTypeFieldImpl("trx_requested_lock_id", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_wait_started", 4, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns.add(new RelDataTypeFieldImpl("trx_weight", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_mysql_thread_id", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_query", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_operation_state", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_tables_in_use", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_tables_locked", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_lock_structs", 11, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("trx_lock_memory_bytes", 12, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_rows_locked", 13, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_rows_modified", 14, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("trx_concurrency_tickets", 15,
                typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_isolation_level", 16,
            typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("trx_unique_checks", 17, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns
            .add(new RelDataTypeFieldImpl("trx_foreign_key_checks", 18,
                typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("trx_last_foreign_key_error", 19, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("trx_adaptive_hash_latched", 20, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("trx_adaptive_hash_timeout", 21, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("trx_is_read_only", 22, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("trx_autocommit_non_locking", 23, typeFactory.createSqlType(SqlTypeName.INTEGER)));

        return typeFactory.createStructType(columns);
    }
}
