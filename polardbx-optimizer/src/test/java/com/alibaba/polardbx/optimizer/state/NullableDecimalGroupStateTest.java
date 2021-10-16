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

package com.alibaba.polardbx.optimizer.state;

import com.alibaba.polardbx.common.datatype.Decimal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NullableDecimalGroupStateTest {

    @Test
    public void test() {
        final Decimal[] values = new Decimal[] {
            null,
            Decimal.fromString("3.14"),
            Decimal.ZERO,
            Decimal.fromString("99999999999999999999999999999999999999999999999999999999999999999"),
            null
        };

        NullableDecimalGroupState state = new NullableDecimalGroupState(100);
        for (int i = 0; i < values.length; i++) {
            final Decimal value = values[i];
            state.appendNull();
            if (value != null) {
                state.set(i, value);
            }
        }

        values[2] = Decimal.fromLong(1);
        state.set(2, values[2]);
        values[3] = Decimal.fromString("7712.025316455696202531645569620253");
        state.set(3, values[3]);

        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(state.isNull(i));
                assertEquals(values[i], state.get(i));
            } else {
                assertTrue(state.isNull(i));
            }
        }
    }
}
