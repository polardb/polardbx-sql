/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TtlTaskSqlBuilderTest {

    @Test
    public void testAddParamsToTDDL() {
        String originalString = "/*+TDDL:cmd_extra(SOCKET_TIMEOUT=1800000)*/";
        String expectedModifiedString =
            "/*+TDDL:cmd_extra(SOCKET_TIMEOUT=1800000,SKIP_DDL_TASKS=\"WaitColumnarTableAlterPartitionTask,WaitColumnarTableCreationTask\")*/";
        String modifiedString = TtlTaskSqlBuilder.addCciHint(originalString,
            "SKIP_DDL_TASKS=\"WaitColumnarTableAlterPartitionTask,WaitColumnarTableCreationTask\"");
        assertEquals(expectedModifiedString, modifiedString);
    }
}
