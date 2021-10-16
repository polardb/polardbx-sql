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

import org.junit.Assert;
import org.junit.Test;

/**
 * @author arnkore 2016-08-04 19:37
 */
public class TCharUtilsTest {
    
    @Test
    public void chineseCharTest() {
        Assert.assertTrue(TCharUtils.isChinese('简')); // 中文简体
        Assert.assertTrue(TCharUtils.isChinese('簡')); // 中文繁体
        Assert.assertTrue(TCharUtils.isChinese('國')); // 中文繁体
        Assert.assertFalse(TCharUtils.isChinese('a')); // ASCII
        Assert.assertFalse(TCharUtils.isChinese('マ')); // 日文
        Assert.assertFalse(TCharUtils.isChinese('λ')); // 希腊文
    }

    @Test
    public void chinesePunctuationTest() {
        Assert.assertTrue(TCharUtils.isChinesePunctuation('，'));
        Assert.assertFalse(TCharUtils.isChinesePunctuation(','));
        Assert.assertTrue(TCharUtils.isChinesePunctuation('＿'));
        Assert.assertTrue(TCharUtils.isChinesePunctuation('。'));
        Assert.assertFalse(TCharUtils.isChinesePunctuation('.'));
    }
}
