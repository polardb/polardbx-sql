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

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.lang.reflect.Field;

public class CollectVariableSyncAction implements ISyncAction {
    // varKey should be the member variable in DynamicConfig,
    // not the one in ConnectionProperties.
    private String varKey;

    public CollectVariableSyncAction(String varKey) {
        this.varKey = varKey;
    }

    public String getVarKey() {
        return varKey;
    }

    public void setVarKey(String varKey) {
        this.varKey = varKey;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("Value");
        result.addColumn("Value", DataTypes.StringType);

        try {
            Class<DynamicConfig> clazz = DynamicConfig.class;
            Field field = clazz.getDeclaredField(varKey);
            field.setAccessible(true);
            Object obj = field.get(DynamicConfig.getInstance());
            result.addRow(new Object[] {obj.toString()});
        } catch (Throwable t) {
            // ignore, result will be empty.
        }

        return result;
    }
}
