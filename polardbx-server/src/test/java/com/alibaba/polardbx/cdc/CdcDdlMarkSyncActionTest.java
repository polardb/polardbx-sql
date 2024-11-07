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

package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.utils.ExecUtils.resultSetToList;

public class CdcDdlMarkSyncActionTest {

    @Test
    public void testSyncOnLeaderNode() {

        CdcDdlMarkSyncAction action = new CdcDdlMarkSyncAction(
            10L, "CREATE_TABLE", "testSchema", "testTable", "CREATE TABLE test", "metaInfo",
            CdcDdlMarkVisibility.Protected, "", true);

        ResultCursor resultCursor = action.buildReturnCursor(123456L);

        List<Map<String, Object>> result = resultSetToList(resultCursor);

        Assert.assertEquals(1, resultCursor.getReturnColumns().size());
        Assert.assertEquals(1, result.size());

        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(123456L, result.get(0).get("COMMIT_TSO"));
    }

}
