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

package com.alibaba.polardbx.optimizer.datastruct;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

public class ObjectSegmentArrayList implements SegmentArrayList {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ObjectSegmentArrayList.class).instanceSize();

    private List<Object> arrays;

    public ObjectSegmentArrayList(int capacity) {
        this.arrays = new ArrayList<>(capacity);
    }

    public void add(Object value) {
        arrays.add(value);
    }

    public void set(int index, Object value) {
        arrays.set(index, value);
    }

    public Object get(int index) {
        return arrays.get(index);
    }

    public int size() {
        return arrays.size();
    }

    @Override
    public long estimateSize() {
        if (arrays.size() == 0) {
            return INSTANCE_SIZE;
        }
        DataType type = DataTypes.StringType;
        try {
            type = DataTypeUtil.getTypeOfObject(arrays.get(0));
        } catch (OptimizerException e) {
            // do nothing
        }
        return INSTANCE_SIZE + arrays.size() * DataTypeUtil.estimateTypeSize(type);
    }
}