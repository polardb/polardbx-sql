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

package com.alibaba.polardbx.common.utils.convertor;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author jianghang 2011-7-12 下午01:04:33
 */
public class StringAndEnumTest {

    private ConvertorHelper helper = new ConvertorHelper();

    @Test
    public void testStringAndEnum() {
        Convertor enumToString = helper.getConvertor(TestEnum.class, String.class);
        Convertor stringtoEnum = helper.getConvertor(String.class, TestEnum.class);
        String VALUE = "TWO";

        Object str = enumToString.convert(TestEnum.TWO, String.class); // 数字
        Assert.assertEquals(str, VALUE);
        Object enumobj = stringtoEnum.convert(VALUE, TestEnum.class); // BigDecimal
        Assert.assertEquals(enumobj, TestEnum.TWO);
    }

    public static enum TestEnum {
        ONE, TWO, THREE;
    }
}
