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
import java.util.List;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-02-05 14:56
 */
public class SqlIndexHint extends SqlCall {

    private SqlCharStringLiteral indexKind;

    private SqlCharStringLiteral forOption;

    private SqlNodeList indexList;

    public SqlIndexHint(SqlCharStringLiteral indexKind , SqlCharStringLiteral forOption , SqlNodeList indexList , SqlParserPos pos) {
        super(pos);
        assert indexKind != null : "index kind cannot be null";
        this.indexKind = indexKind;
        this.forOption = forOption;
        assert indexList != null : "indexList cannot be null";
        this.indexList = indexList;
    }

    @Override
    public SqlOperator getOperator() {
        return SqlIndexHintOperator.INDEX_OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> sqlNodes = new ArrayList<>(3);
        sqlNodes.add(indexKind);
        sqlNodes.add(forOption);
        sqlNodes.add(indexList);
        return sqlNodes;
    }

    public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        getOperator().unparse(writer, this, leftPrec, rightPrec);
    }

    @Override
    public String toString() {
        return toSqlString(null).getSql();
    }

    public SqlNodeList getIndexList() {
        return indexList;
    }

    public boolean forceIndex() {
        return "FORCE INDEX".equalsIgnoreCase(this.indexKind.getNlsString().getValue());
    }

    public boolean pagingForceIndex() {
        return "PAGING_FORCE INDEX".equalsIgnoreCase(this.indexKind.getNlsString().getValue());
    }

    public boolean ignoreIndex() {
        return "IGNORE INDEX".equalsIgnoreCase(this.indexKind.getNlsString().getValue());
    }

    public boolean useIndex() {
        return "USE INDEX".equalsIgnoreCase(this.indexKind.getNlsString().getValue());
    }

    public String getIndexKind() {
        return this.indexKind.getNlsString().getValue();
    }
}
