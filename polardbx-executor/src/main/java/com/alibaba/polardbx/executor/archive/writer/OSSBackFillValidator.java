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

package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Check the checksum value from File storage and Innodb.
 */
public class OSSBackFillValidator {
    private String targetSchema;
    private String targetTable;
    private String targetPhySchema;
    private String targetPhyTable;

    private List<FilesRecord> filesRecordList;

    private String sourceSchema;
    private String sourceTable;
    private String sourcePhySchema;
    private String sourcePhyTable;

    private String partName;
    private ValidatorBound bound;

    public OSSBackFillValidator(String targetSchema, String targetTable, String targetPhySchema, String targetPhyTable,
                                List<FilesRecord> filesRecordList, String sourceSchema, String sourceTable,
                                String sourcePhySchema, String sourcePhyTable, String partName,
                                ValidatorBound bound) {
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.targetPhySchema = targetPhySchema;
        this.targetPhyTable = targetPhyTable;
        this.filesRecordList = filesRecordList;
        this.sourceSchema = sourceSchema;
        this.sourceTable = sourceTable;
        this.sourcePhySchema = sourcePhySchema;
        this.sourcePhyTable = sourcePhyTable;
        this.partName = partName;
        this.bound = bound;
    }

    /**
     * Compute and compare the check sum from file storage and innodb.
     * The check sum is nullable.
     *
     * @param context execution context (without parameters)
     * @return validate result info.
     */
    public ValidationResult validate(ExecutionContext context) {
        // 1. merge files hash value
        OrderInvariantHasher orderInvariantHasher = new OrderInvariantHasher();
        filesRecordList.stream().map(FilesRecord::getFileHash).forEach(orderInvariantHasher::add);
        Long fileMergeHash = orderInvariantHasher.getResult();

        // 2. physical table checksum
        String checksumSql = bound.getChecksumSql(partName);
        Long innodbHash = executeSql(context, checksumSql);

        // 3. compare checksum
        boolean checkSuccess = (fileMergeHash == null && innodbHash == null)
            || (fileMergeHash != null && innodbHash != null && fileMergeHash.compareTo(innodbHash) == 0);

        // 4. check count (inconsistent snapshot)
        Long fileRowCount = filesRecordList.stream().mapToLong(FilesRecord::getTableRows).sum();
        String countSql = bound.getCountSql(partName);
        Long innodbRowCount = executeSql(context, countSql);

        return new ValidationResult(checkSuccess, checksumSql, fileMergeHash, innodbHash, fileRowCount, innodbRowCount);
    }

    private Long executeSql(ExecutionContext context, String logicalSql) {
        if (context.getParams() == null) {
            context.setParams(new Parameters());
        }
        RelNode plan = Planner.getInstance().plan(logicalSql, context).getPlan();
        Cursor result = ExecutorHelper.execute(plan, context);
        Row row;
        while ((row = result.next()) != null) {
            return row.getLong(0);
        }
        return null;
    }

    public class ValidationResult {
        private boolean checkSuccess;
        private String checkSql;
        private Long fileHash;
        private Long innodbHash;
        private Long fileRowCount;
        private Long innodbRowCount;

        public ValidationResult(boolean checkSuccess, String checkSql,
                                Long fileHash, Long innodbHash,
                                Long fileRowCount, Long innodbRowCount) {
            this.checkSuccess = checkSuccess;
            this.checkSql = checkSql;
            this.fileHash = fileHash;
            this.innodbHash = innodbHash;
            this.fileRowCount = fileRowCount;
            this.innodbRowCount = innodbRowCount;
        }

        public boolean isCheckSuccess() {
            return checkSuccess;
        }

        @Override
        public String toString() {
            return "ValidationResult{" +
                "checkSuccess=" + checkSuccess +
                ", checkSql='" + checkSql + '\'' +
                ", fileHash='" + fileHash + '\'' +
                ", innodbHash='" + innodbHash + '\'' +
                ", fileRowCount='" + fileRowCount + '\'' +
                ", innodbRowCount='" + innodbRowCount + '\'' +
                ", sourceSchema='" + sourceSchema + '\'' +
                ", sourceTable='" + sourceTable + '\'' +
                ", sourcePhySchema='" + sourcePhySchema + '\'' +
                ", sourcePhyTable='" + sourcePhyTable + '\'' +
                ", targetSchema='" + targetSchema + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", targetPhySchema='" + targetPhySchema + '\'' +
                ", targetPhyTable='" + targetPhyTable + '\'' +
                '}';
        }
    }

    /**
     * Store the checksum construction info.
     */
    public static class ValidatorBound {
        private static final String[][] LOCAL_PARTITION_QUERY_FORMAT = {
            {
                "select %s from %s partition(%s)",
                "select %s from %s partition(%s) where %s < %s"
            },
            {
                "select %s from %s partition(%s) where %s >= %s",
                "select %s from %s partition(%s) where %s >= %s and %s < %s"
            }
        };

        private String table;
        private String column;
        private String lowerBoundInclusive;
        private String upperBoundExclusive;

        public ValidatorBound(String table, String column, String lowerBound, String upperBound) {
            this.table = table;
            this.column = column;
            this.lowerBoundInclusive = lowerBound;
            this.upperBoundExclusive = upperBound;
        }

        public String getChecksumSql(String partName) {
            return getQuerySql(partName, "check_sum(*)");
        }

        public String getCountSql(String partName) {
            return getQuerySql(partName, "count(*)");
        }

        public String getQuerySql(String partName, String aggFunc) {
            if (lowerBoundInclusive == null) {
                if (upperBoundExclusive == null) {
                    // bad case: unexpected bound.
                    throw new IllegalArgumentException(String.format("unexpected bound: [null, null)"));
                } else if ("MAX_VALUE".equalsIgnoreCase(upperBoundExclusive)) {
                    // bad case: scan all
                    return String.format(LOCAL_PARTITION_QUERY_FORMAT[0][0], aggFunc, table, partName);
                } else {
                    // normal case: no lower bound, upper bound exists.
                    return String.format(LOCAL_PARTITION_QUERY_FORMAT[0][1], aggFunc, table, partName, column,
                        upperBoundExclusive);
                }
            } else {
                if (upperBoundExclusive == null) {
                    // bad case: unexpected bound.
                    throw new IllegalArgumentException(
                        String.format("unexpected bound: [" + lowerBoundInclusive + ", null)"));
                } else if ("MAX_VALUE".equalsIgnoreCase(upperBoundExclusive)) {
                    // normal case: lower bound exists, no upper bound
                    return String.format(LOCAL_PARTITION_QUERY_FORMAT[1][0], aggFunc, table, partName, column,
                        lowerBoundInclusive);
                } else {
                    // normal case: lower bound and upper bound exist.
                    return String.format(LOCAL_PARTITION_QUERY_FORMAT[1][1], aggFunc, table, partName, column,
                        lowerBoundInclusive, column,
                        upperBoundExclusive);
                }
            }

        }
    }
}
