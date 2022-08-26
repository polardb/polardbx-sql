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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

@Ignore
public class FailPointSetTest extends BaseDdlEngineTestCase {

    @Before
    public void before() {
        JdbcUtil.executeQuerySuccess(tddlConnection, "set @fp_clear=true");
    }

    @Test
    public void testEnable() {
        String key = "fp_key1";
        String value = "value1";
        enableFailPoint(key, value);
        List<Pair<String, String>> failPoints = showFailPoints();
        assertFailPointExists(failPoints, key, value);
        assertFailPointNotExists(failPoints, value, key);
        assertFailPointNotExists(failPoints, key + "_random_suffix", value);
        assertFailPointNotExists(failPoints, key, value + "_random_suffix");
    }

    @Test
    public void testDisable() {
        String key = "fp_key1";
        String value = "value1";
        enableFailPoint(key, value);
        List<Pair<String, String>> failPoints = showFailPoints();
        assertFailPointExists(failPoints, key, value);

        disableFailPoint(key);
        failPoints = showFailPoints();
        assertFailPointNotExists(failPoints, key, value);
    }

    @Test
    public void testClear() {
        enableFailPoint("fp_key1", "value1");
        List<Pair<String, String>> failPoints = showFailPoints();
        assertFailPointExists(failPoints, "fp_key1", "value1");

        enableFailPoint("fp_key2", "value2");
        failPoints = showFailPoints();
        assertFailPointExists(failPoints, "fp_key2", "value2");

        clearFailPoints();
        failPoints = showFailPoints();
        assertFailPointNotExists(failPoints, "fp_key1", "value1");
        assertFailPointNotExists(failPoints, "fp_key2", "value2");
    }

    private void assertFailPointExists(List<Pair<String, String>> failPoints, String key, String value) {
        for (Pair<String, String> p : failPoints) {
            if (StringUtils.equals(p.getKey(), key) && StringUtils.equals(p.getValue(), value)) {
                return;
            }
        }
        Assert.fail();
    }

    private void assertFailPointNotExists(List<Pair<String, String>> failPoints, String key, String value) {
        for (Pair<String, String> p : failPoints) {
            if (StringUtils.equals(p.getKey(), key) && StringUtils.equals(p.getValue(), value)) {
                Assert.fail();
            }
        }
    }

}