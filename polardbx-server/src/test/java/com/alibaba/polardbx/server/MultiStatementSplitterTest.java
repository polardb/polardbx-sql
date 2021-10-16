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

package com.alibaba.polardbx.server;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class MultiStatementSplitterTest {

    @Test
    public void testSimple() {
        assertSplitResult("s1;s2", "s1;", "s2");
        assertSplitResult("s1;s2;", "s1;", "s2;");
    }

    @Test
    public void testSingleQuoteString() {
        assertSplitResult("s'fo;o'1;s2", "s'fo;o'1;", "s2");
        assertSplitResult("s'fo\\'o'1;s2", "s'fo\\'o'1;", "s2");
        assertSplitResult("s'fo\"o'1;s2", "s'fo\"o'1;", "s2");
        assertSplitResult("s''''1;s2", "s''''1;", "s2");
    }

    @Test
    public void testDoubleQuoteString() {
        assertSplitResult("s\"fo;o\"1;s2", "s\"fo;o\"1;", "s2");
        assertSplitResult("s\"fo\\\"o\"1;s2", "s\"fo\\\"o\"1;", "s2");
        assertSplitResult("s\"fo'o\"1;s2", "s\"fo'o\"1;", "s2");
        assertSplitResult("s\"\"\"\"1;s2", "s\"\"\"\"1;", "s2");
    }

    @Test
    public void testInlineComment() {
        assertSplitResult("-- foo\ns1;s2", "-- foo\ns1;", "s2");
        assertSplitResult("#fo;o\ns1;s2", "#fo;o\ns1;", "s2");
        assertSplitResult("-- fo'o\ns1;s2", "-- fo'o\ns1;", "s2");

        assertSplitResult("s1;s2-- foo", "s1;", "s2-- foo");
        assertSplitResult("s1;s2#f;oo", "s1;", "s2#f;oo");
        assertSplitResult("s1;s2-- f'oo", "s1;", "s2-- f'oo");

        assertSplitResult("s1;s2--f;oo", "s1;", "s2--f;", "oo");
        assertSplitResult("s1;s2#f;oo", "s1;", "s2#f;oo");
    }

    @Test
    public void testMultiLineComment() {
        assertSplitResult("s/*fo;o*/1;s2", "s/*fo;o*/1;", "s2");
        assertSplitResult("s/*fo;'o*/';'1;s2", "s/*fo;'o*/';'1;", "s2");
    }

    @Test
    public void testEndWithEmpty() {
        assertSplitResult("s1;s2;\n", "s1;", "s2;");
        assertSplitResult("s1;s2; ", "s1;", "s2;");
        assertSplitResult("s1; ;s2;", "s1;", "s2;");
    }

    private void assertSplitResult(String raw, String... results) {
        assertArrayEquals(results, split(raw));
    }

    private String[] split(String query) {
        List<ByteString> results = new MultiStatementSplitter(ByteString.from(query)).split();
        return results.stream().map(ByteString::toString).toArray(String[]::new);
    }

}