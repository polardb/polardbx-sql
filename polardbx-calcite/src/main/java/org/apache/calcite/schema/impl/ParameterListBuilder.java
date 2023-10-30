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

package org.apache.calcite.schema.impl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.util.ArrayList;
import java.util.List;

public class ParameterListBuilder {
    final List<FunctionParameter> builder = new ArrayList<>();

    public ImmutableList<FunctionParameter> build() {
        return ImmutableList.copyOf(builder);
    }

    public ParameterListBuilder add(final RelDataType type, final String name) {
        return add(type, name, false);
    }

    public ParameterListBuilder add(final RelDataType type, final String name,
                                    final boolean optional) {
        final int ordinal = builder.size();
        builder.add(
            new FunctionParameter() {
                @Override
                public int getOrdinal() {
                    return ordinal;
                }

                @Override
                public String getName() {
                    return name;
                }

                @Override
                public RelDataType getType(RelDataTypeFactory typeFactory) {
                    return type;
                }

                @Override
                public boolean isOptional() {
                    return optional;
                }
            });
        return this;
    }
}