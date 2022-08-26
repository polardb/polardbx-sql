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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class DdlLimitTest extends AsyncDDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "ddl_limit_test";
    private static final String LONG_IDENTIFIER_NAME =
        "test1234567890123456789012345678901234567890123456789012345678901";

    @Test
    public void testTableLimits() {
        // Long table name (more than 64 characters)
        String sql = String.format("create table %s(c1 int)", LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");

        // Rename to long table name (more than 64 characters)
        sql = String.format("create table %s(c1 int)", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("rename table %s to %s", TABLE_NAME, LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");
        sql = String.format("drop table if exists %s", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Very long table comment (more than 2048 characters).
        String longComment =
            "1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjk"
                + "fjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweio"
                + "urioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjk"
                + "lwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsd"
                + "weoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfj"
                + "dldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjkle"
                + "wjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioud"
                + "kfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiur"
                + "weiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jr"
                + "ewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfk"
                + "ljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275d"
                + "jlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekw"
                + "jiklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweu"
                + "rioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfklje"
                + "woiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908"
                + "290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkl"
                + "djsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd128738789"
                + "4275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewj"
                + "rlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweuri"
                + "oweurioudkfjkfjwkljewjrlekwjklewjjda";
        sql = String.format("create table %s(c1 int) comment '%s'", TABLE_NAME, longComment);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Comment for table '" + TABLE_NAME + "' is too long");

        // Too many table partitions (more than default 128 table partitions).
        sql = String.format("create table %s(c1 int) dbpartition by hash(c1) tbpartition by hash(c1) tbpartitions 129",
            TABLE_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "The number of table partitions '129' exceeds the upper limit '128'");
    }

    @Test
    public void testColumnLimits() {
        // Long column name (more than 64 characters).
        String sql = String.format("create table %s(%s int)", TABLE_NAME, LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");

        // Very long column comment (more than 1024 characters).
        String longComment =
            "1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjk"
                + "fjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweio"
                + "urioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjk"
                + "lwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsd"
                + "weoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfj"
                + "dldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjkle"
                + "wjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiurweiourioweurioweurioud"
                + "kfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jrewjklwjfkldjfkljewoiur"
                + "weiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfkljsdweoiur20u2908290jr"
                + "ewjklwjfkldjfkljewoiurweiourioweurioweurioudkfjkfjwkljewjrlekwjklewjjd1287387894275djlfjdldkjfkldjfkldjsfk";
        sql = String.format("create table %s(c1 int comment '%s')", TABLE_NAME, longComment);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Comment for field 'c1' is too long");

        StringBuilder buf = new StringBuilder();
        buf.append("create table %s(c1 int");
        for (int i = 2; i < 1020; i++) {
            buf.append(", ").append("c").append(i).append(" int");
        }
        buf.append(")");
        sql = String.format(buf.toString(), TABLE_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Too many columns");
    }

    @Test
    public void testSequenceLimits() {
        // Long sequence name (more than 128 characters).
        String longSeqName = LONG_IDENTIFIER_NAME + LONG_IDENTIFIER_NAME;
        String sql = String.format("create sequence %s", longSeqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + longSeqName + "' is too long");

        // Rename to a long sequence name (more than 128 characters).
        sql = "drop sequence aaa";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } catch (Throwable t) {

        }

        sql = "create sequence aaa";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("rename sequence aaa to %s", longSeqName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + longSeqName + "' is too long");
    }

    @Test
    public void testMiscLimits() {
        // Long constraint name for unique key (more than 64 characters).
        String sql = String.format("create table %s(c1 int not null, constraint %s unique key (c1))", TABLE_NAME,
            LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");

        // Long unique key name (more than 64 characters).
        sql = String.format("create table %s(c1 int not null, unique key %s (c1))", TABLE_NAME, LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");

        // Both long constraint and unique key names (more than 64 characters).
        sql = String.format("create table %s(c1 int not null, constraint %s unique key %s (c1))", TABLE_NAME,
            LONG_IDENTIFIER_NAME, LONG_IDENTIFIER_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Identifier name '" + LONG_IDENTIFIER_NAME + "' is too long");
    }

}
