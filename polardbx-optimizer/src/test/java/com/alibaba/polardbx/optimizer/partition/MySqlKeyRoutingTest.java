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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.google.common.primitives.UnsignedLongs;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import io.airlift.slice.Slices;
import org.junit.Test;

/**
 * @author chenghui.lch
 */
public class MySqlKeyRoutingTest {
    
    public MySqlKeyRoutingTest() {
        
    }
    
    @Test
    public void testKeyRouting() {
        CollationHandler collationHandler = CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.BINARY).getCollationHandler();
        long a = collationHandler.hashcode(Slices.wrappedBuffer("abc".getBytes()));
        long b = collationHandler.hashcode(Slices.wrappedBuffer("abc ".getBytes()));
        long c = collationHandler.hashcode(Slices.wrappedBuffer("abc  ".getBytes()));

        // 49572422
        //-157408730
        //1757155046
        System.out.println(a);
        System.out.println(b);
        System.out.println(c);

        // 6
        // 6
        // 6
        System.out.println(UnsignedLongs.remainder(a, 1024L));
        System.out.println(UnsignedLongs.remainder(b, 1024L));
        System.out.println(UnsignedLongs.remainder(c, 1024L));
    }
}
