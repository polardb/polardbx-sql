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

package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;

public class ServerParseClearTest {

    private static final ParseClearTestCase[] CASES = new ParseClearTestCase[] {
        new ParseClearTestCase("SLOW", ServerParseClear.SLOW),
        new ParseClearTestCase("SLOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("S3 CACHE", ServerParseClear.S3_CACHE),
        new ParseClearTestCase("S3OTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("HEATMAP_CACHE", ServerParseClear.HEATMAP_CACHE),
        new ParseClearTestCase("HOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("ABS CACHE", ServerParseClear.ABS_CACHE),
        new ParseClearTestCase("ABOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("PLANCACHE", ServerParseClear.PLANCACHE),
        new ParseClearTestCase("PLOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("NFS CACHE", ServerParseClear.NFS_CACHE),
        new ParseClearTestCase("NOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("ALL CACHE", ServerParseClear.ALLCACHE),
        new ParseClearTestCase("ALOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("PROCEDURE CACHE", ServerParseClear.PROCEDURE_CACHE),
        new ParseClearTestCase("POTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("EXTERNAL_DISK CACHE", ServerParseClear.EXTERNAL_DISK_CACHE),
        new ParseClearTestCase("EOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("OSS CACHE", ServerParseClear.OSSCACHE),
        new ParseClearTestCase("OOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("FUNCTION CACHE", ServerParseClear.FUNCTION_CACHE),
        new ParseClearTestCase("FOTHER", ServerParseClear.OTHER),
        new ParseClearTestCase("ALL", ServerParseClear.OTHER),
        new ParseClearTestCase("", ServerParseClear.OTHER),
        new ParseClearTestCase("  ", ServerParseClear.OTHER),
    };

    static class ParseClearTestCase {
        ByteString sql;
        int expected;

        public ParseClearTestCase(String sql, int expected) {
            this.sql = createByteString(sql);
            this.expected = expected;
        }
    }

    private static ByteString createByteString(String sql) {
        return new ByteString(sql.getBytes(), 0, sql.length(), Charset.defaultCharset());
    }

    @Test
    public void testParse() {
        for (ParseClearTestCase clearTestCase : CASES) {
            Assert.assertEquals(String.format("parse failed for %s", clearTestCase.sql), clearTestCase.expected,
                ServerParseClear.parse(clearTestCase.sql, 0));
        }
    }
}