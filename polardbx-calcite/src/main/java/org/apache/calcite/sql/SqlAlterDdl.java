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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author dylan
 */
public abstract class SqlAlterDdl extends SqlDdl {

    /** Creates a SqlCreate. */
    public SqlAlterDdl(SqlOperator operator, SqlParserPos pos) {
        super(operator, pos);
    }

    public SqlNode getName() {
        return name;
    }

    /**
     * @return the identifier for the target table of the update
     */
    @Override
    public SqlNode getTargetTable() {
        return name;
    }


    /**
     * @return the identifier for the target table of the update
     */
    @Override
    public void setTargetTable(SqlNode sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateDdl(this , validator.getUnknownType(), scope);
    }
}