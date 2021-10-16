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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.util.Pair;

import java.util.List;

public class ConstraintValidator {

    public static void validateConstraintLimits(SqlCreateTable sqlCreateTable) {
        final SqlIndexDefinition primaryKey = sqlCreateTable.getPrimaryKey();
        final List<Pair<SqlIdentifier, SqlIndexDefinition>> uniqueKeys = sqlCreateTable.getUniqueKeys();

        // Check the name length of primary key constraint.
        if (primaryKey != null && primaryKey.getIndexName() != null) {
            LimitValidator.validateConstraintNameLength(primaryKey.getIndexName().getLastName());
        }

        // Check the name lengths of unique key constraints.
        if (uniqueKeys != null && uniqueKeys.size() > 0) {
            for (org.apache.calcite.util.Pair<SqlIdentifier, SqlIndexDefinition> uniqueKey : uniqueKeys) {
                if (uniqueKey != null) {
                    if (uniqueKey.getKey() != null) {
                        LimitValidator.validateConstraintNameLength(uniqueKey.getKey().getLastName());
                    } else if (uniqueKey.getValue().getIndexName() != null) {
                        LimitValidator.validateConstraintNameLength(uniqueKey.getValue().getIndexName().getLastName());
                    }
                }
            }
        }
    }

    public static void validateConstraintLimits(SqlAlterTable sqlAlterTable) {
        final List<SqlAlterSpecification> alterItems = sqlAlterTable.getAlters();

        // Check the name lengths of unique key constraints.
        if (alterItems != null && alterItems.size() > 0) {
            for (SqlAlterSpecification alterItem : alterItems) {
                if (alterItem instanceof SqlAddUniqueIndex) {
                    SqlAddUniqueIndex uniqueIndex = (SqlAddUniqueIndex) alterItem;
                    if (uniqueIndex.getIndexName() != null) {
                        LimitValidator.validateConstraintNameLength(uniqueIndex.getIndexName().getLastName());
                    } else {
                        SqlIndexDefinition indexDefinition = uniqueIndex.getIndexDef();
                        if (indexDefinition.getIndexName() != null) {
                            LimitValidator.validateConstraintNameLength(indexDefinition.getIndexName().getLastName());
                        }
                    }
                }
            }
        }
    }

}
