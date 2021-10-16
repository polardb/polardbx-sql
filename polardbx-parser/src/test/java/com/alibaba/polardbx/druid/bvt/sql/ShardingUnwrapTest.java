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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.sql.visitor.SQLASTOutputVisitor;
import junit.framework.TestCase;

/**
 * Created by wenshao on 13/08/2017.
 */
public class ShardingUnwrapTest extends TestCase {
    SQLASTOutputVisitor visitor = new SQLASTOutputVisitor(new StringBuffer());

    public void test_sharding_unwrap() throws Exception {

        assertEquals("t_like_count", visitor.unwrapShardingTable("t_like_count0057"));
        assertEquals("t_like_count", visitor.unwrapShardingTable("`t_like_count0057`"));
        assertEquals("t_like_count", visitor.unwrapShardingTable("\"t_like_count0057\""));
    }

    public void test_sharding_unwrap_2() throws Exception {
        assertEquals("t_like_count", visitor.unwrapShardingTable("t_like_count_0057"));
        assertEquals("t_like_count", visitor.unwrapShardingTable("`t_like_count_0057`"));
        assertEquals("t_like_count", visitor.unwrapShardingTable("\"t_like_count_0057\""));
    }

    public void test_sharding_unwrap_3() throws Exception {
        assertEquals("fc_sms", visitor.unwrapShardingTable("fc_sms_0011_201704"));
    }

    public void test_sharding_unwrap_4() throws Exception {
        assertEquals("ads_tb_sycm_eff_slr_itm_1d_s015_p", visitor.unwrapShardingTable("ads_tb_sycm_eff_slr_itm_1d_s015_p033"));
    }

    public void test_sharding_unwrap_5() throws Exception {
        assertEquals("t", visitor.unwrapShardingTable("t_00"));
        assertEquals("t", visitor.unwrapShardingTable("t_1"));
    }
    //
}
