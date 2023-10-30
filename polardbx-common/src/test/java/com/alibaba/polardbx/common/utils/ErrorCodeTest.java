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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.junit.Test;

public class ErrorCodeTest {

    @Test
    public void testOutput() {
        System.out.println(ErrorCode.ERR_TABLE_NOT_EXIST.getMessage("hello"));
    }

    @Test
    public void testErrorCodeMatch() {
        String msg1 = "ERR-CODE: [PXC-4501][ERR_OPTIMIZER] optimize error by ERR-CODE:Table 'tg1' doesn't exist";
        Assert.assertTrue(ErrorCode.match(msg1));

        String msg2 = "Table 'tg1' doesn't exist";
        Assert.assertTrue(!ErrorCode.match(msg2));

        String msg3 = "ERR-CODE: [PXC-4501][ERR_OPTIMIZER] Table 'tg1' doesn't exist ";
        Assert.assertTrue(ErrorCode.match(msg3));

        Assert.assertTrue(!ErrorCode.match(null));

        String msg5 = "";
        Assert.assertTrue(!ErrorCode.match(msg5));
    }

    @Test
    public void testErrorCodeExtract() {
        String msg1 = "ERR-CODE: [PXC-4501][ERR_OPTIMIZER] optimize error by ERR-CODE:Table 'tg1' doesn't exist";
        Assert.assertTrue(ErrorCode.extract(msg1) == 4501);

        String msg2 = "Table 'tg1' doesn't exist";
        Assert.assertTrue(ErrorCode.extract(msg2) == -1);

        String msg3 = "ERR-CODE: [PXC-4501][ERR_OPTIMIZER] Table 'tg1' doesn't exist ";
        Assert.assertTrue(ErrorCode.extract(msg3) == 4501);

        Assert.assertTrue(ErrorCode.extract(null) == -1);

        String msg5 = "";
        Assert.assertTrue(ErrorCode.extract(msg5) == -1);
    }
}
