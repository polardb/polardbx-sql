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

package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

public class ExplainExecuteTest extends ReadBaseTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    public ExplainExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectTest() {
        String sql = "select * from " + baseOneTableName;
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectWithPartitionFilterTest() {
        String sql = "select * from " + baseOneTableName + " where pk=1";
        assertThat(explainExecuteXplan(sql)).isEqualTo(useXproto(tddlConnection));
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateTest() {
        String sql = "update " + baseOneTableName + " set varchar_test='a'";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    @Test
    public void explainJoinTest() {
        String sql = "select * from %s a join %s b on a.varchar_test = b.varchar_test and a.pk=1";
        assertThat(explainExecuteXplan(String.format(sql, baseOneTableName, baseTwoTableName))).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateWithPartitionFilterTest() {
        String sql = "update " + baseOneTableName + " set varchar_test='a' where pk=1";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteTest() {
        String sql = "delete " + baseOneTableName;
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteWithPartitionFilterTest() {
        String sql = "delete " + baseOneTableName + " where varchar_test='a'";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    private boolean explainExecuteXplan(String sql) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain execute " + sql, tddlConnection), false,
                ImmutableList.of());
        boolean useXplan = (!StringUtils.isEmpty(res.get(0).get(11))) && res.get(0).get(11).contains("Using XPlan");
        for (List<String> result : res) {
            assertThat(useXplan == ((!StringUtils.isEmpty(result.get(11))) && result.get(11).contains("Using XPlan")))
                .isTrue();
            if (useXplan) {
                assertThat(result.get(5)).contains(result.get(6));
                assertThat(result.get(6)).isNotEmpty();
                assertThat(Double.valueOf(result.get(10))).isAtMost(100D);
                assertThat(Double.valueOf(result.get(10))).isAtLeast(0D);
            }
        }
        return useXplan;
    }
}
