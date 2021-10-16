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

package com.alibaba.polardbx.druid.sql.ast;

import java.util.Comparator;

public class SQLExprComparor implements Comparator<SQLExpr> {
    public final static SQLExprComparor instance = new SQLExprComparor();

    @Override
    public int compare(SQLExpr a, SQLExpr b) {
        return compareTo(a, b);
    }

    public static int compareTo(SQLExpr a, SQLExpr b) {
        if (a == null && b == null) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (a.getClass() == b.getClass() && a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }

        return a.getClass().getName().compareTo(b.getClass().getName());
    }

}
