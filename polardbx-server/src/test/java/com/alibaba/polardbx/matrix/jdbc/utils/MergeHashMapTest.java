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

package com.alibaba.polardbx.matrix.jdbc.utils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MergeHashMapTest {

    @Test
    public void test() {
        Map<String, Object> base = ImmutableMap.<String, Object>builder()
            .put("a", 1).put("b", 2).put("c", 3).build();
        MergeHashMap<String, Object> merged = new MergeHashMap<>(base);

        assertEquals(2, merged.put("b", null));
        assertEquals(3, merged.put("c", 30));
        assertNull(merged.put("d", 40));

        assertEquals(1, merged.get("a"));
        assertNull(merged.get("b"));
        assertEquals(30, merged.get("c"));
        assertEquals(40, merged.get("d"));

        assertEquals(4, merged.size());
        assertFalse(merged.isEmpty());
        assertTrue(merged.containsKey("b"));
    }
}