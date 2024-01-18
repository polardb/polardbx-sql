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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.List;

import static org.apache.calcite.util.Pair.zip;

public class TypeKnownScalarFunction implements ScalarFunction {
    final RelDataType returnType;
    final List<RelDataType> inputTypes;
    final List<String> inputNames;

    public TypeKnownScalarFunction(RelDataType returnType, List<RelDataType> inputTypes, List<String> inputNames) {
        this.returnType = returnType;
        this.inputNames = inputNames;
        this.inputTypes = inputTypes;
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return returnType;
    }

    @Override
    public List<FunctionParameter> getParameters() {
        ParameterListBuilder paramsBuilder =
            new ParameterListBuilder();
        for (Pair<RelDataType, String> item : zip(inputTypes, inputNames)) {
            paramsBuilder.add(item.left, item.right);
        }
        return paramsBuilder.build();
    }
}
