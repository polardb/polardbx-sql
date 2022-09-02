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

package com.alibaba.polardbx.qatest.failpoint.recoverable.newpartition;

import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.failpoint.base.BaseFailPointTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

/**
 * @author chenghui.lch
 */
public class BasePartitionedTableFailPointTestCase extends BaseFailPointTestCase {

    public static final String DEFAULT_PARTITIONING_DEFINITION =
        " partition by key(id) partitions 8";
    protected String host;
    protected String password;
    protected String dbName;
    protected Connection conn;

    protected boolean enableFpEachDdlTaskBackAndForth = true;
    protected boolean enableFpEachDdlTaskExecuteTwiceTest = true;
    protected boolean enableFpRandomPhysicalDdlExceptionTest = true;
    protected boolean enableFpRandomFailTest = true;
    protected boolean enableFpRandomSuspendTest = true;

    public BasePartitionedTableFailPointTestCase() {
    }

    @Before
    public void doBefore() {
        clearFailPoints();
    }

    @After
    public void doAfter() {
        clearFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_BACK_AND_FORTH() {

        if (!enableFpEachDdlTaskBackAndForth) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_BACK_AND_FORTH, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_EACH_DDL_TASK_EXECUTE_TWICE() {

        if (!enableFpEachDdlTaskExecuteTwiceTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_EACH_DDL_TASK_EXECUTE_TWICE, "true");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_PHYSICAL_DDL_EXCEPTION() {

        if (!enableFpRandomPhysicalDdlExceptionTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_PHYSICAL_DDL_EXCEPTION, "30");
        execDdlWithFailPoints();
    }

    @Test
    public void test_FP_RANDOM_FAIL() {

        if (!enableFpRandomFailTest) {
            return;
        }

        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_FAIL, "30");
        execDdlWithFailPoints();
        doClean();
    }

    @Test
    public void test_FP_RANDOM_SUSPEND() {

        if (!enableFpRandomSuspendTest) {
            return;
        }
        doPrepare();
        enableFailPoint(FailPointKey.FP_RANDOM_SUSPEND, "30,3000");
        execDdlWithFailPoints();
    }

    protected void doPrepare() {
    }

    protected void execDdlWithFailPoints() {
    }

    protected void doClean() {
    }

}
