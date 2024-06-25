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
package com.alibaba.polardbx.config;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.common.truth.Truth;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusterLoaderTest {
    @Test
    public void testApplyProperties() {
        // with MAPPING_TO_MYSQL_ERROR_CODE='{4006:1146}
        DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "{4006:1146}");
        Truth.assertThat(DynamicConfig.getInstance().getErrorCodeMapping().get(4006)).isEqualTo(1146);

        // without MAPPING_TO_MYSQL_ERROR_CODE='{4006:1146}
        DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "");
        Truth.assertThat(DynamicConfig.getInstance().getErrorCodeMapping()).isEmpty();
    }
}
