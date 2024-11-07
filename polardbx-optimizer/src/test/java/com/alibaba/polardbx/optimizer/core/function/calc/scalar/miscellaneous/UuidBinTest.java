/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.miscellaneous;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UuidBinTest {

    @Test
    public void testBinToUuid() {
        UUID uuid = UUID.randomUUID();
        BinToUuid function = new BinToUuid();
        UuidToBin function1 = new UuidToBin();

        byte[] uuidBytes1 = (byte[]) function1.compute(new Object[] {uuid}, new ExecutionContext());
        byte[] uuidBytes2 = (byte[]) function1.compute(new Object[] {uuid, 1}, new ExecutionContext());
        Slice slice1 = Slices.wrappedBuffer(uuidBytes1);
        Slice slice2 = Slices.wrappedBuffer(uuidBytes2);

        Object result = function.compute(new Object[] {slice1}, new ExecutionContext());
        assertEquals(uuid.toString(), result);

        Object result1 = function.compute(new Object[] {slice1, 0}, new ExecutionContext());
        assertEquals(uuid.toString(), result1);

        Object result2 = function.compute(new Object[] {slice2, 1}, new ExecutionContext());
        assertEquals(uuid.toString(), result2);
    }

    @Test
    public void testBinToUuidWithNullArgument() {
        BinToUuid function = new BinToUuid();
        Object result = function.compute(new Object[] {null}, new ExecutionContext());
        assertNull(result);
    }

    @Test
    public void testUuidToBinWithNullArgument() {
        UuidToBin function = new UuidToBin();
        Object result = function.compute(new Object[] {null}, new ExecutionContext());
        assertNull(result);
    }

}
