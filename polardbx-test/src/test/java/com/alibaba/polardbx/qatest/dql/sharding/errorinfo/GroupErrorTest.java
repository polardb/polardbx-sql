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

package com.alibaba.polardbx.qatest.dql.sharding.errorinfo;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * Created by fangwu on 2017/3/22.
 */

public class GroupErrorTest extends CrudBasedLockTestCase {
    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays
            .asList(ExecuteTableName.allBaseTypeOneTable(ExecuteTableName.GEMO_TEST));
    }

    public GroupErrorTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    /**
     * 检查单表及分表环境下，各ATOM报错返回的错误提示是否包含了DBLINK信息
     */
    @Test
    public void testMulGroupError() {
        String sql = "insert into " + baseOneTableName
            + "(pk,geom,pot) values(12,'34','jfdk'),(1,'34','jfdk'),(67,'34','jfdk'),(99,'34','jfdk'),(90,'34','jfdk'),(73,'34','jfdk'),(13,'34','jfdk'),(15,'34','jfdk')";///*TDDL:node='andor_mysql_group_3'*/
        //exe
        executeErrorAssert(tddlConnection, sql, null, "ATOM");

    }

}
