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
package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.google.common.truth.Truth;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TConnectionTest {
    @Test
    public void newExecutionContextTest() {

        final TConnection tConnection = new TConnection(new TDataSource());

        // OUTPUT_MYSQL_ERROR_CODE = false
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isFalse();

        // OUTPUT_MYSQL_ERROR_CODE = true
        ParamManager.setBooleanVal(tConnection.getExecutionContext().getParamManager().getProps(),
            ConnectionParams.OUTPUT_MYSQL_ERROR_CODE, true, false);
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isTrue();
    }

    /**
     * Tests whether the method correctly sets the MASTER property when given a valid 'groupindex:0' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForMaster() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:0";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.MASTER));
    }

    /**
     * Tests whether the method correctly sets the SLAVE property when given a valid 'groupindex:1' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForSlave() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:1";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.SLAVE));
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an empty string as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithEmptyString() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given null as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithNull() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = null;

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an invalid input as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithInvalidInput() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "invalid_input";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }
}
