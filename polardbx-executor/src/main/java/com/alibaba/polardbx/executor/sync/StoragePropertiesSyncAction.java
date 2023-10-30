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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * @author yaozhili
 */
public class StoragePropertiesSyncAction implements ISyncAction {
    public StoragePropertiesSyncAction() {

    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("STORAGE_PROPERTIES");
        result.addColumn("PROPERTIES", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.BooleanType);

        Optional.ofNullable(ExecutorContext.getContext("polardbx")).ifPresent(context -> {
            final StorageInfoManager manager = context.getStorageInfoManager();

            // Get all boolean member variable of StorageInfoManager.
            Class<?> clazz = manager.getClass();
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (field.getType().equals(boolean.class)) {
                    field.setAccessible(true);
                    boolean value = false;
                    try {
                        value = field.getBoolean(manager);
                    } catch (IllegalAccessException e) {
                        // Ignore this value.
                        continue;
                    }
                    result.addRow(new Object[] {field.getName(), value});
                }
            }
        });

        return result;
    }
}
