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

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Created by chuanqin on 2019/7/31. Entity for function signature
 */
public class FunctionSignature {

    private List<RelDataType> parameterTypes;
    private String name;

    public FunctionSignature(List<RelDataType> parameterTypes, String name) {
        this.parameterTypes = parameterTypes;
        this.name = name;
    }

    public static FunctionSignature getFunctionSignature(List<RelDataType> parameterTypes, String name) {
        return new FunctionSignature(parameterTypes, name);
    }

    public List<RelDataType> getParameterTypes() {
        return parameterTypes;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof FunctionSignature) {
            if (!((FunctionSignature) obj).getName().equalsIgnoreCase(name)) {
                return false;
            }
            if (parameterTypes == null && ((FunctionSignature) obj).getParameterTypes() == null) {
                return true;
            }
            if (parameterTypes != null && ((FunctionSignature) obj).getParameterTypes() != null
                && ((FunctionSignature) obj).getParameterTypes().size() == parameterTypes.size()) {
                for (int i = 0; i < parameterTypes.size(); i++) {
                    if (!parameterTypes.get(i)
                        .getSqlTypeName()
                        .getName()
                        .equalsIgnoreCase(((FunctionSignature) obj).getParameterTypes()
                            .get(i)
                            .getSqlTypeName()
                            .getName())) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = ((name == null) ? 0 : name.toUpperCase().hashCode());
        return result;
    }
}
