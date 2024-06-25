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

package com.alibaba.polardbx.common;

import junit.framework.TestCase;

public class EngineTest extends TestCase {

    public void testOf() {
        assertEquals(Engine.OSS, Engine.of("OSS"));
        assertEquals(Engine.INNODB, Engine.of(""));
        boolean catchException = false;
        try {
            Engine.of("NON_EXIST_ENGINE");
        } catch (Throwable t) {
            catchException = true;
            assertTrue(t.getMessage().equalsIgnoreCase("Unknown engine name:NON_EXIST_ENGINE"));
        }
        if (!catchException) {
            throw new AssertionError("Engine.of() method should fail");
        }
    }

    public void testHasCache() {
        assertTrue(Engine.hasCache(Engine.OSS));
        assertTrue(Engine.hasCache(Engine.EXTERNAL_DISK));
        assertTrue(Engine.hasCache(Engine.NFS));
        assertTrue(Engine.hasCache(Engine.S3));
        assertTrue(Engine.hasCache(Engine.ABS));
        assertFalse(Engine.hasCache(Engine.INNODB));
        assertFalse(Engine.hasCache(null));
    }

    public void testIsFileStore() {
        assertTrue(Engine.isFileStore(Engine.OSS));
        assertTrue(Engine.isFileStore(Engine.EXTERNAL_DISK));
        assertTrue(Engine.isFileStore(Engine.LOCAL_DISK));
        assertTrue(Engine.isFileStore(Engine.NFS));
        assertTrue(Engine.isFileStore(Engine.S3));
        assertTrue(Engine.isFileStore(Engine.ABS));
        assertFalse(Engine.isFileStore(Engine.INNODB));
        assertFalse(Engine.isFileStore((Engine) null));
    }

    public void testSupportColumnar() {
        assertTrue(Engine.supportColumnar(Engine.OSS));
        assertTrue(Engine.supportColumnar(Engine.EXTERNAL_DISK));
        assertTrue(Engine.supportColumnar(Engine.LOCAL_DISK));
        assertTrue(Engine.supportColumnar(Engine.NFS));
        assertTrue(Engine.supportColumnar(Engine.S3));
        assertTrue(Engine.supportColumnar(Engine.ABS));
        assertFalse(Engine.supportColumnar(Engine.INNODB));
        assertFalse(Engine.supportColumnar((Engine) null));
    }

    public void testTestIsFileStore() {
        assertTrue(Engine.isFileStore("OSS"));
    }
}