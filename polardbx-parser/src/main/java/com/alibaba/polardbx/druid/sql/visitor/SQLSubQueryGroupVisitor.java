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

package com.alibaba.polardbx.druid.sql.visitor;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SQLSubQueryGroupVisitor extends SQLASTVisitorAdapter {
    private final DbType dbType;

    protected Map<Long, List<SQLSubqueryTableSource>> tableSourceMap = new LinkedHashMap<Long, List<SQLSubqueryTableSource>>();

    public SQLSubQueryGroupVisitor(DbType dbType) {
        this.dbType = dbType;
    }

    public boolean visit(SQLSubqueryTableSource x) {
        String sql = SQLUtils.toSQLString(x.getSelect(), dbType);
        long hashCode64 = FnvHash.fnv1a_64(sql);
        List<SQLSubqueryTableSource> list = tableSourceMap.get(hashCode64);
        if (list == null) {
            list = new ArrayList<SQLSubqueryTableSource>();
            tableSourceMap.put(hashCode64, list);
        }
        list.add(x);

        return true;
    }

    public Collection<List<SQLSubqueryTableSource>> getGroupedSubqueryTableSources() {
        return tableSourceMap.values();
    }

}
