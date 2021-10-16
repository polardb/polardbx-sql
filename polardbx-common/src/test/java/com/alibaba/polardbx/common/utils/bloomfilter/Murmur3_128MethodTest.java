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

package com.alibaba.polardbx.common.utils.bloomfilter;

import com.google.common.hash.HashCode;
import com.alibaba.polardbx.util.bloomfilter.Murmur3_128Method;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class Murmur3_128MethodTest {
    @Test
    public void testHashMethodReset() {
        TddlHasher hasher = Murmur3_128Method.create().newHasher();

        hasher.putBytes("abcdefghijklmn".getBytes(StandardCharsets.UTF_8));
        hasher.putLong(1000L);
        hasher.putDouble(2000.123);
        HashCode hashCode1 = hasher.hash();

        hasher.putBytes("abcdefghijklmn".getBytes(StandardCharsets.UTF_8));
        hasher.putLong(1000L);
        hasher.putDouble(2000.123);
        HashCode hashCode2 = hasher.hash();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testHashMethodResetWithDifferentValues() {
        TddlHasher hasher = Murmur3_128Method.create().newHasher();

        hasher.putBytes("abcdefghijklmn".getBytes(StandardCharsets.UTF_8));
        hasher.putLong(1000L);
        hasher.putDouble(2000.123);
        HashCode hashCode1 = hasher.hash();

        hasher.putBytes("abc".getBytes(StandardCharsets.UTF_8));
        hasher.putLong(1L);
        HashCode hashCode2 = hasher.hash();

        assertNotEquals(hashCode1, hashCode2);
    }
}
