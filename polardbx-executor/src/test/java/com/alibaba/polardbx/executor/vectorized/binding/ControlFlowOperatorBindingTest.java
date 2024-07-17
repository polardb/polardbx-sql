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

package com.alibaba.polardbx.executor.vectorized.binding;

import org.junit.Test;

public class ControlFlowOperatorBindingTest extends BindingTestBase {

    @Test
    public void testCoalesce() {
        testProject("select coalesce(integer_test, 1, varchar_test) from test")
            .showTrees();

        testProject("select ifnull(integer_test, varchar_test) from test")
            .showTrees();
    }

    @Test
    public void testNullIf() {
        // todo
        testProject("select nullif(integer_test, 1) from test")
            .showTrees();
    }
}
