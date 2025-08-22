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
package com.alibaba.polardbx.planner.dml;

import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PushJoinPruningTest extends ParameterizedTestCommon {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    private static MockedStatic<ConnPoolConfigManager> connPoolConfigManagerMockedStatic;

    public PushJoinPruningTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @BeforeClass
    public static void beforeClass() {
        connPoolConfigManagerMockedStatic = Mockito.mockStatic(ConnPoolConfigManager.class);
        connPoolConfigManagerMockedStatic.when(ConnPoolConfigManager::buildDefaultConnPoolConfig).thenCallRealMethod();

        final ConnPoolConfig mockedConnPoolConfig = mock(ConnPoolConfig.class);
        when(mockedConnPoolConfig.isStorageDbXprotoEnabled()).thenReturn(true);

        final ConnPoolConfigManager mockedConnPoolConfigManager = mock(ConnPoolConfigManager.class);
        when(mockedConnPoolConfigManager.getConnPoolConfig()).thenReturn(mockedConnPoolConfig);

        connPoolConfigManagerMockedStatic.when(ConnPoolConfigManager::getInstance)
            .thenReturn(mockedConnPoolConfigManager);
    }

    @AfterClass
    public static void afterClass() {
        if (null != connPoolConfigManagerMockedStatic) {
            connPoolConfigManagerMockedStatic.close();
        }
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PushJoinPruningTest.class);
    }
}
