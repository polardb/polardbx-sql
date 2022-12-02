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