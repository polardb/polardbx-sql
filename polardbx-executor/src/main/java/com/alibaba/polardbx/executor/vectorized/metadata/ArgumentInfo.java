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

package com.alibaba.polardbx.executor.vectorized.metadata;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class ArgumentInfo {
    private final DataType<?> type;
    private final ArgumentKind kind;

    public ArgumentInfo(DataType<?> type,
                        ArgumentKind kind) {
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(kind);
        this.type = type;
        this.kind = kind;
    }

    public DataType<?> getType() {
        return type;
    }

    public ArgumentKind getKind() {
        return kind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ArgumentInfo that = (ArgumentInfo) o;
        return DataTypeUtil.equalsSemantically(type, that.type) &&
            getKind() == that.getKind();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType().getClass(), getKind());
    }

    @Override
    public String toString() {
        return "ArgumentInfo {" +
            "type=" + type +
            ", kind=" + kind +
            '}';
    }
}