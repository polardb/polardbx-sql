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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;

import java.sql.Connection;

public class ReadBaseTestCase extends BaseTestCase {

    protected Connection mysqlConnection;
    protected Connection tddlConnection;
    protected String baseOneTableName;
    protected String baseTwoTableName;
    protected String baseThreeTableName;
    protected String baseFourTableName;
    protected String polardbxOneDB;
    protected String polardbxOneDB2;
    protected String mysqlOneDB;

    @Before
    public void beforeDmlBaseTestCase() {
        this.polardbxOneDB = PropertiesUtil.polardbXDBName1(usingNewPartDb());
        this.polardbxOneDB2 = PropertiesUtil.polardbXDBName2(usingNewPartDb());
        this.mysqlOneDB = PropertiesUtil.mysqlDBName1();
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
    }
}

