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

package com.alibaba.polardbx.manager.parser;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.manager.response.AbstractSimpleSelect;
import com.alibaba.polardbx.manager.response.MultiColumnSimpleSelect;
import com.alibaba.polardbx.manager.response.NonsupportedStatement;
import com.alibaba.polardbx.manager.response.SelectX;
import com.alibaba.polardbx.manager.response.SingleColumnSimpleSelect;
import com.alibaba.polardbx.server.util.ParseUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * 简单select语句的支持
 * 1. select 'x';
 * 2. select session变量1, session变量2; (session变量的形式支持两种 select @@sessionVariable or select @@session.sessionVariable)
 * 3. 支持别名, 仅限于session变量
 *
 * @author xianmao.hexm 2011-5-9 下午04:16:19
 * @author arnkore 2016-12-27
 */
public final class ManagerParseSelect {
    private static final char[][] SIMPLE_SELECT_STMTS = new char[][] {
        "VERSION_COMMENT".toCharArray(),
        "AUTO_INCREMENT_INCREMENT".toCharArray(),
        "TX_ISOLATION".toCharArray(),
        "TX_READ_ONLY".toCharArray(),
        "TRANSACTION_ISOLATION".toCharArray(),
        "TRANSACTION_READ_ONLY".toCharArray(),
        "MAX_ALLOWED_PACKET".toCharArray(),
        "CHARACTER_SET_CLIENT".toCharArray(),
        "CHARACTER_SET_CONNECTION".toCharArray(),
        "CHARACTER_SET_RESULTS".toCharArray(),
        "CHARACTER_SET_SERVER".toCharArray(),
        "COLLATION_CONNECTION".toCharArray(),
        "COLLATION_SERVER".toCharArray(),
        "INIT_CONNECT".toCharArray(),
        "INTERACTIVE_TIMEOUT".toCharArray(),
        "LICENSE".toCharArray(),
        "LOWER_CASE_TABLE_NAMES".toCharArray(),
        "NET_BUFFER_LENGTH".toCharArray(),
        "NET_WRITE_TIMEOUT".toCharArray(),
        "QUERY_CACHE_SIZE".toCharArray(),
        "QUERY_CACHE_TYPE".toCharArray(),
        "SQL_MODE".toCharArray(),
        "SYSTEM_TIME_ZONE".toCharArray(),
        "TIME_ZONE".toCharArray(),
        "WAIT_TIMEOUT".toCharArray(),
        "PERFORMANCE_SCHEMA".toCharArray()
    };

    public static AbstractSimpleSelect parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
            case ' ':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case '@':
            case '\'':
            case '"':
                return select2Check(stmt, i);
            default:
                return new NonsupportedStatement(stmt, null, null);
            }
        }
        return new NonsupportedStatement(stmt, null, null);
    }

    private static AbstractSimpleSelect select2Check(String stmt, int offset) {
        if (stmt.length() > ++offset) {
            switch (stmt.charAt(offset)) {
            case '@':
                return simpleSelectCheck(stmt, offset - 1);
            case 'x':
            case 'X':
                return selectXCheck(stmt, offset);
            default:
                return new NonsupportedStatement(stmt, null, null);
            }
        }
        return new NonsupportedStatement(stmt, null, null);
    }

    /**
     * 检查是否为select 'x'语句
     */
    private static SingleColumnSimpleSelect selectXCheck(String stmt, int offset) {
        String origColumnName = String.valueOf(stmt.charAt(offset));
        if (stmt.length() > ++offset) {
            char ch = stmt.charAt(offset);
            if (stmt.length() == ++offset && (ch == '\'' || ch == '\"')) {
                return new SelectX(origColumnName);
            }
        }

        return new NonsupportedStatement(stmt, null, null);
    }

    /**
     * 检查是否是返回单列的简单simple select语句
     */
    private static SingleColumnSimpleSelect singleColumnCheck(String subStmt) {
        // FIXME 有时间再重构
        String[] asList = new String[] {" as ", " As ", " aS ", " AS "};
        String asKeyword = " ";
        for (String asKw : asList) {
            if (subStmt.contains(asKw)) {
                asKeyword = asKw;
            }
        }

        String[] partColumns = subStmt.split(asKeyword);
        String origColumn = partColumns[0].trim();
        String aliasColumn = origColumn;
        if (partColumns.length > 1) {
            aliasColumn = partColumns[1].trim();
        }

        int offset = 0;
        String stmt = TStringUtil.replaceWithIgnoreCase(origColumn, "@@", "");
        stmt = TStringUtil.replaceWithIgnoreCase(stmt, "session.", "");
        return singleColumnCheck(stmt, offset, origColumn, aliasColumn);
    }

    /**
     * 检查是否是返回单列的简单simple select语句
     */
    private static SingleColumnSimpleSelect singleColumnCheck(String stmt, int offset, String origColumn,
                                                              String aliasColumn) {
        // select 'x', druid use this to validate query legacy.
        for (char[] simpleSelect : SIMPLE_SELECT_STMTS) {
            if (ParseUtil.compare(stmt, offset, simpleSelect)) {
                int length = offset + simpleSelect.length;
                if (stmt.length() > length && stmt.charAt(length) != ' ') {
                    continue;
                }

                return reflectSimpleSelect(simpleSelect, origColumn, aliasColumn);
            }
        }

        return new NonsupportedStatement(origColumn, aliasColumn);
    }

    /**
     * 采用反射获取该类的构造器
     */
    private static SingleColumnSimpleSelect reflectSimpleSelect(char[] simpleSelectStmt, String origColumn,
                                                                String aliasColumn) {
        String humpStyleClassName = TStringUtil.convertToHumpStr(String.valueOf(simpleSelectStmt).toLowerCase());
        String className = "com.alibaba.polardbx.manager.response.Select" + StringUtils.capitalize(humpStyleClassName);
        try {
            Class clz = Class.forName(className);
            Constructor constructor = clz.getConstructor(String.class, String.class);
            return (SingleColumnSimpleSelect) constructor.newInstance(origColumn, aliasColumn);
        } catch (ClassNotFoundException e) {
            GeneralUtil.nestedException("Doesn't exists class " + className);
        } catch (NoSuchMethodException e) {
            GeneralUtil.nestedException(e.getMessage());
        } catch (IllegalAccessException e) {
            GeneralUtil.nestedException(e.getMessage());
        } catch (InstantiationException e) {
            GeneralUtil.nestedException(e.getMessage());
        } catch (InvocationTargetException e) {
            GeneralUtil.nestedException(e.getMessage());
        }

        return new NonsupportedStatement(origColumn, aliasColumn);
    }

    /**
     * 检查是否是返回多列的简单simple select语句
     */
    private static MultiColumnSimpleSelect multiColumnCheck(String[] subStms) {
        List<SingleColumnSimpleSelect> scssList = Lists.newArrayList();
        for (String subStmt : subStms) {
            SingleColumnSimpleSelect scss = singleColumnCheck(subStmt.trim());
            scssList.add(scss);
        }

        return new MultiColumnSimpleSelect(scssList);
    }

    /**
     * 检查是否是简单simple select语句
     */
    private static AbstractSimpleSelect simpleSelectCheck(String stmt, int offset) {
        String columnPart = stmt.substring(offset);
        String[] subStms = columnPart.split(",");
        if (subStms.length > 1) {
            return multiColumnCheck(subStms);
        } else {
            return singleColumnCheck(subStms[0]);
        }
    }
}
