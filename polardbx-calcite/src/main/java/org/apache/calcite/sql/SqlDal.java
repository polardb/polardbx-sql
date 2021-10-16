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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author chenmo.cm
 * @date 2018/6/20 上午12:24
 */
public abstract class SqlDal extends SqlCall implements SqlHint, SqlWithTable, SqlWithDb {

    protected SqlNodeList hints;

    protected List<SqlNode> operands = Collections.emptyList();

    protected List<Integer> tableIndexes;

    protected List<Integer> dbIndexes;

    protected List<Boolean> dbWithFrom;

    public SqlDal(SqlParserPos pos){
        super(pos);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return operands;
    }

    @Override
    public SqlNodeList getHints() {
        return hints;
    }

    @Override
    public void setHints(SqlNodeList hints) {
        this.hints = hints;
    }

    @Override
    public void setTableName(String tableName) {
        setTableName(new SqlIdentifier(tableName, SqlParserPos.ZERO));
    }

    @Override
    public SqlNode getTableName() {
        if (GeneralUtil.isNotEmpty(tableIndexes)) {
            return this.operands.get(tableIndexes.get(0));
        } else {
            return null;
        }
    }

    @Override
    public void setTableName(SqlNode tableName) {
        if (GeneralUtil.isNotEmpty(tableIndexes)) {
            this.operands.set(tableIndexes.get(0), tableName);
        }
    }

    @Override
    public List<SqlNode> getTableNames() {
        return collect(tableIndexes);
    }

    @Override
    public void setDbName(String dbName) {
        setDbName(new SqlIdentifier(dbName, SqlParserPos.ZERO));
    }

    @Override
    public SqlNode getDbName() {
        if (GeneralUtil.isNotEmpty(dbIndexes)) {
            return this.operands.get(dbIndexes.get(0));
        } else {
            return null;
        }
    }

    @Override
    public void setDbName(SqlNode dbName) {
        if (GeneralUtil.isNotEmpty(dbIndexes)) {
            this.operands.set(dbIndexes.get(0), dbName);
            if (null == dbName && isDbWithFrom(0)) {
                this.operands.set(dbIndexes.get(0) - 1, null);
            }
        }
    }

    @Override
    public List<SqlNode> getDbNames() {
        return collect(dbIndexes);
    }

    @Override
    public Boolean isDbWithFrom() {
        if (GeneralUtil.isNotEmpty(dbWithFrom)) {
            return dbWithFrom.get(0);
        } else {
            return false;
        }
    }

    @Override
    public Boolean isDbWithFrom(int index) {
        if (GeneralUtil.isNotEmpty(dbWithFrom) && dbWithFrom.size() > index) {
            return this.dbWithFrom.get(index);
        } else {
            return false;
        }
    }

    private List<SqlNode> collect(List<Integer> paramIndexes) {
        if (GeneralUtil.isNotEmpty(paramIndexes)) {
            List<SqlNode> result = new ArrayList<>(paramIndexes.size());
            for (Integer index : paramIndexes) {
                result.add(this.operands.get(index));
            }
            return result;
        } else {
            return null;
        }
    }
}
