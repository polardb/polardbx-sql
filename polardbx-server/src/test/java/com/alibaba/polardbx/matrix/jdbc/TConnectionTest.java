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
import com.alibaba.polardbx.common.properties.ParamManager;
import com.google.common.truth.Truth;
import org.junit.Test;

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
}
