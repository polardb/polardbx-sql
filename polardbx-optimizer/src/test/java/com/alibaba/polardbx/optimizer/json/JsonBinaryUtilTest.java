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

package com.alibaba.polardbx.optimizer.json;

import com.alibaba.polardbx.optimizer.json.exception.JsonTooLargeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * 与MySQL二进制序列化结果相比较
 */
public class JsonBinaryUtilTest {

    @Test
    public void testJsonScalar() throws JsonTooLargeException {
        List<String> jsonList = new ArrayList<String>() {{
            add("\"ABCD你\"");
            add("123");
            add("123999999");
            add("true");
            add("false");
            add("null");
        }};
        List<byte[]> jsonBytesList = new ArrayList<byte[]>() {{
            add(new byte[] {12, 7, 65, 66, 67, 68, -28, -67, -96});
            add(new byte[] {5, 123, 0});
            add(new byte[] {7, -1, 22, 100, 7});
            add(new byte[] {4, 1});
            add(new byte[] {4, 2});
            add(new byte[] {4, 0});
        }};
        validate(jsonList, jsonBytesList);
    }

    @Test
    public void testJsonSmallArray() {
        List<String> jsonList = new ArrayList<String>() {{
            add("[\"ABCD你\", 123, true, \"abc\"]");
        }};
        List<byte[]> jsonBytesList = new ArrayList<byte[]>() {{
            add(new byte[] {
                2, 4, 0, 28, 0, 12, 16, 0, 5, 123, 0, 4, 1, 0,
                12, 24, 0, 7, 65, 66, 67, 68, -28, -67, -96, 3, 97, 98, 99
            });
        }};

        validate(jsonList, jsonBytesList);
    }

    @Test
    public void testJsonSmallObject() {
        List<String> jsonList = new ArrayList<String>() {{
            add("{\"key\": 12}");
            add("{\"key\": \"value\"}");
            add("{\"a\": \"value\", \"b\": 999, \"c\": null}");
        }};
        List<byte[]> jsonBytesList = new ArrayList<byte[]>() {{
            add(new byte[] {
                0, 1, 0, 14, 0, 11, 0, 3, 0, 5, 12, 0,
                107, 101, 121
            });
            add(new byte[] {
                0, 1, 0, 20, 0, 11, 0, 3, 0, 12, 14, 0,
                107, 101, 121, 5, 118, 97, 108, 117, 101
            });
            add(new byte[] {
                0, 3, 0, 34, 0, 25, 0, 1, 0, 26, 0, 1, 0,
                27, 0, 1, 0, 12, 28, 0, 5, -25, 3, 4, 0, 0, 97, 98, 99,
                5, 118, 97, 108, 117, 101
            });
        }};
        validate(jsonList, jsonBytesList);
    }

    @Test
    public void testNestedJson() {
        List<String> jsonList = new ArrayList<String>() {{
            add("[123, {\"a\": 10}]");
            add("[123, {\"a\": 10, \"b\": \"value\"}, 987]");
            add("{\"x\": [1, 2, \"y\"]}");
            add("[\"abc\", [{\"k\": \"10\"}, \"def\"], {\"x\": [1, 2, 5, \"value\"]}]");
        }};
        List<byte[]> jsonBytesList = new ArrayList<byte[]>() {{
            add(new byte[] {
                2, 2, 0, 22, 0, 5, 123, 0, 0, 10,
                0, 1, 0, 12, 0, 11, 0, 1, 0, 5, 10, 0, 97
            });
            add(new byte[] {
                2, 3, 0, 39, 0, 5, 123, 0, 0, 13, 0,
                5, -37, 3, 2, 0, 26, 0, 18, 0, 1, 0, 19, 0, 1,
                0, 5, 10, 0, 12, 20, 0, 97, 98, 5, 118, 97, 108,
                117, 101
            });
            add(new byte[] {
                0, 1, 0, 27, 0, 11, 0, 1, 0, 2, 12, 0, 120,
                3, 0, 15, 0, 5, 1, 0, 5, 2, 0, 12, 13, 0, 1, 121
            });
            add(new byte[] {
                2, 3, 0, 80, 0, 12, 13, 0, 2, 17, 0, 0, 46, 0, 3,
                97, 98, 99, 2, 0, 29, 0, 0, 10, 0, 12, 25, 0, 1, 0,
                15, 0, 11, 0, 1, 0, 12, 12, 0, 107, 2, 49, 48, 3, 100,
                101, 102, 1, 0, 34, 0, 11, 0, 1, 0, 2, 12, 0, 120, 4, 0,
                22, 0, 5, 1, 0, 5, 2, 0, 5, 5, 0, 12, 16, 0, 5, 118, 97,
                108, 117, 101
            });
        }};
        validate(jsonList, jsonBytesList);
    }

    private void validate(List<String> jsonList, List<byte[]> expectedBytesList) {
        Assert.assertEquals(jsonList.size(), expectedBytesList.size());
        for (int i = 0; i < jsonList.size(); i++) {
            Object json = JsonUtil.parse(jsonList.get(i));
            byte[] result = JsonBinaryUtil.toBytes(json, jsonList.get(i).length());
            Assert.assertArrayEquals("Failed in: " + jsonList.get(i), expectedBytesList.get(i), result);
        }
    }
}
