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

package com.alibaba.polardbx.optimizer.utils;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.util.BitSets;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public final class ExecutionPlanProperties {

    public static final int MODIFY_BROADCAST_TABLE = 0;

    public static final int MODIFY_GSI_TABLE = 1;

    public static final int WITH_INDEX_HINT = 2;

    public static final int MODIFY_TABLE = 3;

    public static final int MODIFY_SHARDING_COLUMN = 4;

    public static final int MODIFY_CROSS_DB = 5;

    public static final int MODIFY_SCALE_OUT_GROUP = 6;

    public static final int SELECT_WITH_LOCK = 7;

    public static final int REPLICATE_TABLE = 8;
    /**
     * Simple query which only select single broadcast table
     */
    public static final int ONLY_BROADCAST_TABLE = 9;

    public static final int QUERY = 10;

    public static final int DML = 11;

    public static final int DDL = 12;

    public static final int SCALE_OUT_WRITABLE_TABLE = 13;

    public static final int MODIFY_ONLINE_COLUMN_TABLE = 14;

    public static final int MODIFY_FOREIGN_KEY = 15;

    public static final Set<Integer> MDL_REQUIRED = ImmutableSet
        .of(MODIFY_TABLE, MODIFY_GSI_TABLE, MODIFY_BROADCAST_TABLE, MODIFY_SHARDING_COLUMN, MODIFY_CROSS_DB,
            MODIFY_SCALE_OUT_GROUP, REPLICATE_TABLE);

    public static final Set<Integer> MDL_REQUIRED_POLARDBX = ImmutableSet
        .of(QUERY, DML);

    public static final Set<Integer> XA_REQUIRED = ImmutableSet
        .of(MODIFY_BROADCAST_TABLE, MODIFY_GSI_TABLE, MODIFY_SHARDING_COLUMN, MODIFY_CROSS_DB, MODIFY_SCALE_OUT_GROUP,
            REPLICATE_TABLE);

    public static final Set<Integer> DDL_STATEMENT = ImmutableSet.of(DDL);

    public static final Set<Integer> DML_STATEMENT = ImmutableSet.of(DML);

    private static Integer cachedMaxPropertyValue = null;

    public static int getMaxPropertyValue() {
        if (cachedMaxPropertyValue != null) {
            // cache
            return cachedMaxPropertyValue;
        }
        try {
            Field[] fields = ExecutionPlanProperties.class.getFields(); // 获取所有公共字段

            int maxPropertyValue = Integer.MIN_VALUE;
            for (Field field : fields) {
                if (field.getType() == int.class && (field.getModifiers() & java.lang.reflect.Modifier.STATIC) != 0
                    && (field.getModifiers() & java.lang.reflect.Modifier.FINAL) != 0) {
                    int value = field.getInt(null); // 获取字段的值
                    maxPropertyValue = Math.max(maxPropertyValue, value); // 计算最大值
                }
            }
            int result = maxPropertyValue + 1;
            cachedMaxPropertyValue = result;
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
