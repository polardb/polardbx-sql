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
import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.ast.SQLArrayDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLMapDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLStructDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.MySqlUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SQLDataTypeValidator extends SQLASTVisitorAdapter {
    private long[] supportTypeHashCodes;

    public SQLDataTypeValidator(String[] supportTypes) {
        this.supportTypeHashCodes = FnvHash.fnv1a_64_lower(supportTypes, true);
    }

    public SQLDataTypeValidator(Set<String> typeSet) {
        this.supportTypeHashCodes = new long[typeSet.size()];

        int i = 0;
        for (String type : typeSet) {
            this.supportTypeHashCodes[i++] = FnvHash.fnv1a_64_lower(type);
        }
        Arrays.sort(supportTypeHashCodes);
    }

    public boolean visit(SQLDataType x) {
        validate(x);
        return true;
    }

    public boolean visit(SQLCharacterDataType x) {
        validate(x);
        return true;
    }

    public boolean visit(SQLArrayDataType x) {
        validate(x);
        return true;
    }

    public boolean visit(SQLMapDataType x) {
        validate(x);
        return true;
    }

    public boolean visit(SQLStructDataType x) {
        validate(x);
        return true;
    }

    public void validate(SQLDataType x) {
        long hash = x.nameHashCode64();
        if (Arrays.binarySearch(supportTypeHashCodes, hash) < 0) {
            String msg = "illegal dataType : " + x.getName();

            final SQLObject parent = x.getParent();
            if (parent instanceof SQLColumnDefinition) {
                SQLColumnDefinition column = (SQLColumnDefinition) parent;
                if (column.getName() != null) {
                    msg += ", column " + column.getName();
                }
            }
            throw new FastsqlException(msg);
        }
    }

    private static String[] odpsTypes = null;
    private static String[] hiveTypes = null;
    private static String[] mysqlTypes = null;

    public static SQLDataTypeValidator of(DbType dbType) {
        Set<String> typeSet = null;
        String[] types = null;
        switch (dbType) {
            case mysql: {
                types = mysqlTypes;
                if (types == null) {
                    typeSet = new HashSet<String>();
                    MySqlUtils.loadDataTypes(typeSet);
                }
                break;
            }
            default:
                break;
        }

        if (types == null && typeSet != null) {
            types = typeSet.toArray(new String[typeSet.size()]);
        }

        if (types == null) {
            throw new FastsqlException("dataType " + dbType + " not support.");
        }

        return new SQLDataTypeValidator(types);
    }

    public static void check(SQLStatement stmt) {
        SQLDataTypeValidator v = of(stmt.getDbType());
        stmt.accept(v);
    }

    public static void check(List<SQLStatement> stmtList) {
        if (stmtList.size() == 0) {
            return;
        }

        DbType dbType = stmtList.get(0).getDbType();
        SQLDataTypeValidator v = of(dbType);

        check(stmtList, dbType);
    }

    public static void check(List<SQLStatement> stmtList, DbType dbType) {
        if (stmtList.size() == 0) {
            return;
        }

        SQLDataTypeValidator v = of(dbType);

        for (SQLStatement stmt : stmtList) {
            stmt.accept(v);
        }
    }
}
