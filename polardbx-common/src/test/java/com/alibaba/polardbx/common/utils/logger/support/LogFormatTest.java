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

package com.alibaba.polardbx.common.utils.logger.support;

import junit.framework.TestCase;
import org.junit.Assert;

public class LogFormatTest extends TestCase {

    public void testInlineComment() {
        Assert.assertEquals("/* foo*/select 1;/*bar*/", LogFormat.formatLog("-- foo\nselect 1;#bar"));
        Assert.assertEquals("/*foo*/select 1;/* bar*/", LogFormat.formatLog("#foo\nselect 1;-- bar\n"));
        Assert.assertEquals("/*#foo*/ select 1;/*--bar*/", LogFormat.formatLog("/*#foo*/ select 1;/*--bar*/"));
        Assert.assertEquals("select 1;/* bar*/select 2;", LogFormat.formatLog("select 1;-- bar\nselect 2;"));

        Assert.assertEquals("select 1;/**/select 2;", LogFormat.formatLog("select 1;--\nselect 2;"));
        Assert.assertEquals("select 1;/*\tselect 2;*/", LogFormat.formatLog("select 1;--\tselect 2;"));

        Assert.assertEquals("select 1,'foo#bar'", LogFormat.formatLog("select 1,'foo#bar'"));
        Assert.assertEquals("select 1,'foo-- bar'", LogFormat.formatLog("select 1,'foo-- bar'"));
        Assert.assertEquals("select 1,'foo/*bar*/'", LogFormat.formatLog("select 1,'foo/*bar*/'"));

        Assert.assertEquals("select 1/*foo**bar*/", LogFormat.formatLog("select 1#foo*/bar"));
        Assert.assertEquals("select 1/* foo**bar*/", LogFormat.formatLog("select 1-- foo*/bar"));

        Assert.assertEquals("select 1/*foo#/#bar*/", LogFormat.formatLog("select 1/*foo#/#bar*/"));
    }
}