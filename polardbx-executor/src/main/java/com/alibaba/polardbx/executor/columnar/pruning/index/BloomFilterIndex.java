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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author fangwu
 */
public class BloomFilterIndex extends BaseColumnIndex {
    protected BloomFilterIndex(long rgNum) {
        super(rgNum);
    }

    @Override
    public boolean checkSupport(int columnId, SqlTypeName columnType) {
        return false;
    }

    @Override
    public DataType getColumnDataType(int columnId) {
        return null;
    }
}
