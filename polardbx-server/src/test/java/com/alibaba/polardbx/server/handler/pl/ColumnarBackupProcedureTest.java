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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.common.cdc.CdcDDLContext;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.server.handler.pl.inner.ColumnarBackupProcedure;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class ColumnarBackupProcedureTest {

    @Test
    public void callTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(100L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_backup");

            ColumnarBackupProcedure procedure = new ColumnarBackupProcedure();
            // Execute inner procedure.
            procedure.execute(null, null, cursor);

            Assert.assertEquals(1, cursor.getRows().size());
            Assert.assertEquals(100L, cursor.getRows().get(0).getObject(0));
        }
    }

    @Test
    public void callErrorTest() {
        try (MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class)) {
            CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
            cdcManagerHelperMockedStatic.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);
            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(null);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            ArrayResultCursor cursor = new ArrayResultCursor("columnar_backup");

            ColumnarBackupProcedure procedure = new ColumnarBackupProcedure();
            // Execute inner procedure.
            try {
                procedure.execute(null, null, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }

            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(0L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            try {
                procedure.execute(null, null, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }

            doAnswer(invocation -> {
                CdcDDLContext cdcDDLContext = invocation.getArgument(0);
                cdcDDLContext.setCommitTso(-1L);
                return null;
            }).when(cdcManagerHelper).notifyDdlWithContext(any());

            try {
                procedure.execute(null, null, cursor);
                Assert.fail("should throw exception");
            } catch (Exception ignored) {

            }
        }
    }
}
