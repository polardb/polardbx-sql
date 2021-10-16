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

package com.alibaba.polardbx.druid.bvt.sql.common;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import junit.framework.TestCase;

public class SelectTest_0 extends TestCase {
    public void test_select_0() throws Exception {
        SQLUtils.parseSingleStatement("select idc,room, f0.f0_value as resultValue from (select idc,room, avg(`value`) as f0_value from tsdb.`raptor-pro-root` where ((idc='EU95')) and `timestamp` between '2019-01-17 01:07:20' and '2019-01-17 01:07:35' and obj_type = '30118' and obj_base_metric = '1001' group by idc,room) as f0", null);
    }
}
