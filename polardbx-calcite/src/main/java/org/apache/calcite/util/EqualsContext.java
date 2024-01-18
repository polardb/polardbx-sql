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

package org.apache.calcite.util;
import com.alibaba.polardbx.common.jdbc.Parameters;

import java.util.HashSet;
import java.util.Set;

public class EqualsContext {
    public static final EqualsContext DEFAULT_EQUALS_CONTEXT = new EqualsContext();

    final private boolean genColSubstitute;
    final private Set<Integer> constantParamIndex;
    final private Parameters parameters;
    final String schemaName;
    final String tableName;
    final String idTableName;

    public EqualsContext() {
        this.genColSubstitute = false;
        this.constantParamIndex = new HashSet<>();
        this.parameters = new Parameters();
        this.schemaName = "";
        this.tableName = "";
        this.idTableName = "";
    }

    public EqualsContext(Parameters parameters, String schemaName, String tableName, String idTableName) {
        this.genColSubstitute = true;
        this.constantParamIndex = new HashSet<>();
        this.parameters = parameters;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.idTableName = idTableName;
    }

    public boolean isGenColSubstitute() {
        return genColSubstitute;
    }

    public Set<Integer> getConstantParamIndex() {
        return constantParamIndex;
    }

    public Parameters getParameters() {
        return parameters;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIdTableName() {
        return idTableName;
    }
}
