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

package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;

/**
 * @author chenmo.cm
 * @date 2018/5/21 上午10:56
 */
public class SystemTableNamespace extends AbstractNamespace{
    private final SqlCall current;
    private final SqlValidatorScope scope;

    /**
     * Creates an AbstractNamespace.
     *
     * @param validator     Validator
     * @param enclosingNode Enclosing node
     */
    SystemTableNamespace(SqlValidatorImpl validator, SqlCall current, SqlValidatorScope scope, SqlNode enclosingNode) {
        super(validator, enclosingNode);
        this.current = current;
        this.scope = scope;
    }

    @Override
    protected RelDataType validateImpl(RelDataType targetRowType) {
        if (current instanceof SqlSelect) {
            final SqlSelect fakeSelect = (SqlSelect)this.current;
            //validator.validateSelect(fakeSelect, targetRowType);

            final SqlCall from = (SqlCall)(fakeSelect).getFrom();
            return from.getOperator().deriveType(this.validator, this.scope, from);
        } else {
            return current.getOperator().deriveType(this.validator, this.scope, this.current);
        }
    }

    @Override
    public SqlNode getNode() {
        return current;
    }

    public String getAlias() {
        if (current instanceof SqlShow) {
            return ((SqlShow)current).getShowKind().toString();
        } else {
            return current.getKind().toString();
        }
    }
}
