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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a workaround for columnar to support MySQL SET type while keeping compatible with old versions
 */
public class SetType extends CharType {

    private final ImmutableList<String> setValues;

    public SetType(List<String> setValues) {
        this.setValues = ImmutableList.copyOf(setValues);
    }

    public ImmutableList<String> getSetValues() {
        return setValues;
    }

    public List<String> convertFromBinary(long binaryValue) {
        List<String> resultSet = new ArrayList<>();
        int setIndex = 0;
        while (binaryValue != 0) {
            if ((binaryValue & 1L) == 1L) {
                resultSet.add(setValues.get(setIndex));
            }
            setIndex++;
            binaryValue >>= 1;
        }
        return resultSet;
    }

    // To achieve full compatibility with MySQL, there are more methods to be implemented ...
}
