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

package com.alibaba.polardbx.rule.utils;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.model.AdvancedParameter.Range;

public class AdvancedParameterParserTest {

    @Test
    public void test_正常() {
        String param = "id,1_number,1024";
        AdvancedParameter result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result, AtomIncreaseType.NUMBER, 1, 1024);

        param = "id,1024";
        result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result, AtomIncreaseType.NUMBER, 1, 1024);
    }

    @Test
    public void test_范围() {
        String param = "id,1_number,0_1024|1m_1g";
        AdvancedParameter result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result,
            AtomIncreaseType.NUMBER,
            new AdvancedParameter.Range[] { getRange(0, 1024), getRange(1 * 1000000, 1 * 1000000000) },
            1);

        param = "id,0_1024|1m_1g";
        result = AdvancedParameterParser.getAdvancedParamByParamTokenNew(param, false);
        testResult(result,
            AtomIncreaseType.NUMBER,
            new AdvancedParameter.Range[] { getRange(0, 1024), getRange(1 * 1000000, 1 * 1000000000) },
            1);
    }

    private void testResult(AdvancedParameter result, AtomIncreaseType type, Comparable atomicIncreateValue,
                            Integer cumulativeTimes) {
        Assert.assertEquals(result.atomicIncreateType, type);
        Assert.assertEquals(result.atomicIncreateValue, atomicIncreateValue);
        Assert.assertEquals(result.cumulativeTimes, cumulativeTimes);
    }

    private void testResult(AdvancedParameter result, AtomIncreaseType type, Range[] rangeValue,
                            Integer atomicIncreateValue) {
        Assert.assertEquals(result.atomicIncreateType, type);
        Assert.assertEquals(result.atomicIncreateValue, atomicIncreateValue);
        int i = 0;
        for (Range range : result.rangeArray) {
            Assert.assertEquals(range.start, rangeValue[i].start);
            Assert.assertEquals(range.end, rangeValue[i].end);
            i++;
        }
    }

    private AdvancedParameter.Range getRange(int start, int end) {
        return new AdvancedParameter.Range(start, end);
    }

}
