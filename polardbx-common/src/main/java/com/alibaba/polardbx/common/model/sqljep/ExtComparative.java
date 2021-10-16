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

package com.alibaba.polardbx.common.model.sqljep;

public class ExtComparative extends Comparative {

    private String     columnName;

    public ExtComparative(String name, int function, Object value){
        super(function, value);
        this.columnName = name;
    }

    public String toString() {
        if (getValue() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            if (columnName != null) {
                sb.append(columnName);
                sb.append(":");
            }
            sb.append(getComparisonName(getComparison()));
            sb.append(getValue().toString()).append(")");
            return sb.toString();
        } else {
            return null;
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public ExtComparative clone() {
        return new ExtComparative(this.columnName, this.getComparison(), this.getValue());
    }

}
