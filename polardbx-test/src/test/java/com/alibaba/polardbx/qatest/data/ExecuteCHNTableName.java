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

package com.alibaba.polardbx.qatest.data;

import com.alibaba.polardbx.common.exception.NotSupportException;

/**
 * 操作表
 */
public class ExecuteCHNTableName {

    public final static String ONE_DB_ONE_TB_SUFFIX = "单库";
    //不分库分表
    public final static String ONE_DB_MUTIL_TB_SUFFIX = "单库分表";
    //分库不分表
    public final static String MULTI_DB_ONE_TB_SUFFIX = "分库不分表";
    //分库分表
    public final static String MUlTI_DB_MUTIL_TB_SUFFIX = "分库分表";
    //广播表
    public final static String BROADCAST_TB_SUFFIX = "广播表";

    //以varchar为拆分字段的表，表名加上这个关键字
    public final static String HASH_BY_VARCHAR = "字符串拆分_";

    //以日期为拆分字段的表，表名加上这个关键字
    public final static String HASH_BY_DATE = "日期拆分_";

    public final static String TWO = "二_";

    public final static String THREE = "三_";

    public final static String FOUR = "四_";

    public final static String UPDATE_DELETE_BASE = "中文_客户_";

    public final static String UPDATE_DELETE_BASE_AUTONIC = "中文_客户_自增_";

    public final static String SELECT_BASE = "查询_基础表_";

    /**
     * 所有基本类型的表
     */
    public static String[][] allBaseTypeOneTable(String tablePrefix) {
//        String tablePrefix = classNameChangeToTablePrefix(className);

        String[][] object = {
            //不分库不分表
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
            //不分库分表
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
            //分库分表
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},
            //广播表
            {tablePrefix + BROADCAST_TB_SUFFIX},};

        return object;

    }

    /**
     * 所有基本类型的表, 包含varchar拆分
     */
    public static String[][] allBaseTypeWithStringRuleOneTable(String dbType, String tablePrefix) {

        if (dbType.equals("mysql") || dbType.equals("tddl-server")
            || dbType.equals("embed-server") || dbType.equals("prepare")
            || dbType.equals("compress")) {
            String[][] object = {
                //不分库不分表
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX},
                //不分库分表
                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX},

                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX},
                //分库分表
                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX},
                //分库分表键为string 规则
                {tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX},
                //分库分表键为string 规则
                {tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX},
                //分库分表键为string 规则
                {tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX},
                //广播表
                {tablePrefix + BROADCAST_TB_SUFFIX},};

            return object;
        }

        throw new NotSupportException();

    }

    public static String[][] allBaseTypeTwoTable(String tablePrefix) {

        String[][] object = {
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

        };

        return object;

    }

    public static String[][] allBaseTypeTwoTableWithStringRule(String dbType, String tablePrefix) {

        if (dbType.equals("mysql") || dbType.equals("tddl-server")
            || dbType.equals("embed-server") || dbType.equals("prepare")
            || dbType.equals("compress")) {
            String[][] object = {
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

                {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
                {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

                {
                    tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + ONE_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + BROADCAST_TB_SUFFIX},

                {
                    tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MUlTI_DB_MUTIL_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + BROADCAST_TB_SUFFIX},

                {
                    tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + ONE_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + MULTI_DB_ONE_TB_SUFFIX},
                {
                    tablePrefix + HASH_BY_VARCHAR + MULTI_DB_ONE_TB_SUFFIX,
                    tablePrefix + HASH_BY_VARCHAR + TWO + BROADCAST_TB_SUFFIX},
            };
            return object;
        }
        throw new NotSupportException();
    }

    public static String[][] allBaseTypeTwoStrictSameTable(String tablePrefix) {

        String[][] object = {
            {tablePrefix + ONE_DB_ONE_TB_SUFFIX, tablePrefix + TWO + ONE_DB_ONE_TB_SUFFIX},
            {tablePrefix + ONE_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + ONE_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MUlTI_DB_MUTIL_TB_SUFFIX, tablePrefix + TWO + MUlTI_DB_MUTIL_TB_SUFFIX},
            {tablePrefix + MULTI_DB_ONE_TB_SUFFIX, tablePrefix + TWO + MULTI_DB_ONE_TB_SUFFIX},
            {tablePrefix + BROADCAST_TB_SUFFIX, tablePrefix + TWO + BROADCAST_TB_SUFFIX},

        };
        return object;
    }
}
