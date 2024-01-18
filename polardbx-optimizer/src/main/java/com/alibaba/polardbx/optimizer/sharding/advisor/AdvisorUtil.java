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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BigIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.MediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.NumberType;
import com.alibaba.polardbx.optimizer.core.datatype.SmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UIntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.UMediumIntType;
import com.alibaba.polardbx.optimizer.core.datatype.USmallIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;

/**
 * This class stores global variables
 *
 * @author shengyu
 */
public class AdvisorUtil {
    static public int BROADCAST = -100;

    static public String adviseSql(String fullTableName, String columnName, Integer partitions) {
        String[] split = fullTableName.split("\\.");
        String schemaName = split[0];
        String tableName = split[1];
        try {
            TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).
                getLatestSchemaManager().getTddlRuleManager();
            // to broadcast table
            if (columnName == null) {
                if (tddlRuleManager.isBroadCast(tableName)) {
                    return "";
                }
                return String.format("alter table %s.%s broadcast;\n", schemaName, tableName);
            } else {
                // to shard partition table
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    return String.format("alter table %s.%s PARTITION BY KEY(`%s`) PARTITIONS %d;\n",
                        schemaName, tableName, columnName, partitions);
                } else {
                    if (partitions == null || partitions == 1) {
                        return String.format("alter table %s.%s dbpartition by hash(`%s`);\n",
                            schemaName, tableName, columnName);
                    } else {
                        return String.format(
                            "alter table %s.%s dbpartition by hash(`%s`) tbpartition by hash(`%s`) tbpartitions %d;\n",
                            schemaName, tableName, columnName, columnName, partitions);
                    }
                }
            }
        } catch (NullPointerException e) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, fullTableName);
        }
    }

    static public int getPartitions(String fullTableName) {
        String[] split = fullTableName.split("\\.");
        String schemaName = split[0];
        String tableName = split[1];
        try {
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                return OptimizerContext.getContext(split[0]).
                    getLatestSchemaManager().getTddlRuleManager().getPartitionInfoManager().getPartitionInfo(tableName)
                    .getPartitionBy().getPhysicalPartitions().size();
            }
            TableRule tableRule = OptimizerContext.getContext(split[0]).
                getLatestSchemaManager().getTddlRuleManager().getTableRule(split[1]);
            return tableRule.getActualTbCount() / tableRule.getActualDbCount();
        } catch (NullPointerException e) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, fullTableName);
        }
    }

    enum Mode {LAST, EMPTY, ALL}

    static public Mode PRINT_MODE = Mode.EMPTY;

    static public boolean USE_MAX_ACC = true;

    private static boolean columnHashable(DataType dataType) {
        //don't support time hash yet
        if (dataType instanceof NumberType) {
            if (dataType instanceof TinyIntType) {
                return true;
            }
            if (dataType instanceof UTinyIntType) {
                return true;
            }
            if (dataType instanceof SmallIntType) {
                return true;
            }
            if (dataType instanceof USmallIntType) {
                return true;
            }
            if (dataType instanceof MediumIntType) {
                return true;
            }
            if (dataType instanceof UMediumIntType) {
                return true;
            }
            if (dataType instanceof IntegerType) {
                return true;
            }
            if (dataType instanceof UIntegerType) {
                return true;
            }
            if (dataType instanceof BigIntegerType) {
                return true;
            }
        } else {
            if (dataType instanceof StringType) {
                return true;
            }
            if (dataType instanceof CharType) {
                return true;
            }
            if (dataType instanceof VarcharType) {
                return true;
            }
        }
        return false;
    }

    /**
     * determine whether the column can be used as hash
     *
     * @param tableMeta the table tested
     * @param column the column willing to hash
     * @return true if the column be used as partition key
     */
    public static boolean columnHashable(TableMeta tableMeta, int column) {
        ColumnMeta columnMeta = tableMeta.getAllColumns().get(column);
        boolean newDB = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        if (newDB) {
            return PartitionInfoBuilder.isSupportedPartitionDataType(columnMeta.getDataType());
        } else {
            return AdvisorUtil.columnHashable(columnMeta.getDataType());
        }
    }

    /**
     * check whether a name contains backtick `
     *
     * @param name the name tested
     * @return ture if the name doesn't contain backtick
     */
    public static boolean notBacktick(String name) {
        return !name.contains("`");
    }
}
