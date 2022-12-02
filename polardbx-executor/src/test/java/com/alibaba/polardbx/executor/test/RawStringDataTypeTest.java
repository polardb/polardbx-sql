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

package com.alibaba.polardbx.executor.test;

import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;
import org.weakref.jmx.internal.guava.collect.Lists;

import java.util.function.Function;

/**
 * @author fangwu
 */
public class RawStringDataTypeTest {
    @Test
    public void testRawStringChangeDateType() {
        RawString rs =
            new RawString(Lists.newArrayList("123", 4.5f, "null", null, "488271095309602817", 488271095309602817L));
        Assert.assertTrue(rs.convertType(DataTypes.ULongType::convertFrom).display()
            .equals("Raw(123,4,0,null,488271095309602817,488271095309602817)"));
        Assert.assertTrue(rs.convertType(DataTypes.LongType::convertFrom).display()
            .equals("Raw(123,4,0,null,488271095309602817,488271095309602817)"));
        Assert.assertTrue(
            rs.convertType(DataTypes.StringType::convertFrom).display()
                .equals("Raw('123','4.5','null',null,'488271095309602817','488271095309602817')"));
    }
}
