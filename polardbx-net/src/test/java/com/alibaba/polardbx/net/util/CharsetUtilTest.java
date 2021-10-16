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

package com.alibaba.polardbx.net.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author chenghui.lch 2017年8月9日 下午3:25:43
 * @since 5.0.0
 */
public class CharsetUtilTest {

    @Test
    public void testLatin1() {

        int charsetIndex = 5;
        String charset = "latin1";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));

    }

    @Test
    public void testGbk() {

        int charsetIndex = 87;
        String charset = "gbk";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));
    }

    @Test
    public void testGb2312() {

        int charsetIndex = 24;
        String charset = "gb2312";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));
    }

    @Test
    public void testUtf8() {

        int charsetIndex = 213;
        String charset = "utf8";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));
    }

    @Test
    public void testUtf8mb4() {

        int charsetIndex = 235;
        String charset = "utf8mb4";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));
    }

    @Test
    public void testBinary() {

        int charsetIndex = 63;
        String charset = "binary";
        Assert.assertTrue(charset.equals(CharsetUtil.getCharset(charsetIndex)));
    }
}
