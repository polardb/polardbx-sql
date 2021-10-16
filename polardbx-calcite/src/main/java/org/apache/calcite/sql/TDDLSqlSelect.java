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

/**
 * Created by hongxi.chx on 2017/12/4.
 */
public class TDDLSqlSelect extends SqlSelect implements SqlHint {

    private SqlNodeList hints;

    public TDDLSqlSelect(SqlParserPos pos, SqlNodeList keywordList, SqlNodeList selectList, SqlNode from, SqlNode where,
                         SqlNodeList groupBy, SqlNode having, SqlNodeList windowDecls, SqlNodeList orderBy,
                         SqlNode offset, SqlNode fetch, SqlNodeList hints, LockMode lockMode) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch);
        this.hints = hints;
        this.setLockMode(lockMode);
    }

    public TDDLSqlSelect(SqlParserPos pos, SqlNodeList keywordList, SqlNodeList selectList, SqlNode from, SqlNode where,
                         SqlNodeList groupBy, SqlNode having, SqlNodeList windowDecls, SqlNodeList orderBy,
                         SqlNode offset, SqlNode fetch, SqlNodeList hints, LockMode lockMode,
                         OutFileParams outFileParams) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch,
            outFileParams);
        this.hints = hints;
        this.setLockMode(lockMode);
    }

    private TDDLSqlSelect(SqlParserPos pos, SqlSelect select, SqlNodeList hints, LockMode lockMode) {
        super(pos,
            select.keywordList,
            select.selectList,
            select.from,
            select.where,
            select.groupBy,
            select.having,
            select.windowDecls,
            select.orderBy,
            select.offset,
            select.fetch);
        this.hints = hints;
        this.setLockMode(lockMode);
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
    public SqlNode clone(SqlParserPos pos) {
        final SqlNode clone = super.clone(pos);
        if (clone instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) clone;
            final TDDLSqlSelect tddlSqlSelect = new TDDLSqlSelect(pos, select, getHints(), getLockMode());
            if (!((SqlSelect) clone).getOptimizerHint().getHints().isEmpty()) {
                for (String hint : ((SqlSelect) clone).getOptimizerHint().getHints()) {
                    tddlSqlSelect.getOptimizerHint().addHint(hint);
                }
            }
            return tddlSqlSelect;
        } else {
            return clone;
        }
    }
}
