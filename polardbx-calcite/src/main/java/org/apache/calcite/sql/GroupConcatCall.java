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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;

public class GroupConcatCall extends SqlBasicCall {
    private String separator = null;

    private ArrayList<String> ascOrDescList;
    private ArrayList<SqlNode> orderOperands;

    public GroupConcatCall(
            SqlOperator operator,
            SqlNode[] operands,
            SqlParserPos pos,
            boolean expanded,
            SqlLiteral functionQualifier,
            String separator,
            ArrayList<SqlNode> orderOperands,
            ArrayList<String> ascOrDescList) {
        super(operator, operands, pos, expanded, functionQualifier);
        this.separator = separator;
        this.orderOperands = orderOperands;
        this.ascOrDescList = ascOrDescList;
    }

    public String getSeparator() {
        return separator;
    }

    public ArrayList<String> getAscOrDescList() {
        return ascOrDescList;
    }

    public ArrayList<SqlNode> getOrderOperands() {
        return orderOperands;
    }

    @Override public String computeAttributesString() {
        String result = "";
        if (orderOperands != null && orderOperands.size() != 0) {
            result += " ORDER BY ";
            boolean first = true;
            for (int i = 0; i < orderOperands.size(); i++) {
                SqlNode sqlNode = orderOperands.get(i);
                String ascOrDesc = ascOrDescList.get(i);
                if (first) {
                    first = false;
                } else {
                    result += ", ";
                }
                result += sqlNode.toString() + " " + ascOrDesc;
            }
        }
        if (separator != null) {
            result += " SEPARATOR '" + separator + "'";
        }
        return result;
    }
}
