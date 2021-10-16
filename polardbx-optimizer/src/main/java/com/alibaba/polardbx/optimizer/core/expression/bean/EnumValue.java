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

package com.alibaba.polardbx.optimizer.core.expression.bean;

import com.alibaba.polardbx.optimizer.core.datatype.EnumType;

public class EnumValue {

    public final EnumType type;
    public final String value;

    public EnumValue(EnumType type, String value) {
        this.type = type;
        this.value = value;
    }

    public EnumType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
