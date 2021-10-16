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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.exception.FunctionException;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;

public class NowTest {
    @Test
    public void verifyPrecision() {
        for (int i = 0; i <= 6; i++) {
            checkData(i, (int) Math.pow(10, 9 - i));
        }
    }

    @Test
    public void verifyPrecisionWithoutArgs() {
        Now now = new Now(null, new DateTimeType());
        ExecutionContext ec = new ExecutionContext();

        Timestamp ts = (Timestamp) now.compute(new Object[0], ec);
        assertEquals(0, ts.getNanos() % 1_000_000_000);
    }

    @Test(expected = FunctionException.class)
    public void verifyThrowExceptionWithNegativeArgs() {
        Now now = new Now(null, new DateTimeType());
        ExecutionContext ec = new ExecutionContext();

        now.compute(new Object[] {-1}, ec);
    }

    @Test(expected = FunctionException.class)
    public void verifyThrowExceptionWithTooLargeArgs() {
        Now now = new Now(null, new DateTimeType());
        ExecutionContext ec = new ExecutionContext();

        now.compute(new Object[] {7}, ec);
    }

    private void checkData(int precision, int division) {
        Now now = new Now(null, new DateTimeType());
        ExecutionContext ec = new ExecutionContext();

        Timestamp ts = (Timestamp) now.compute(new Object[] {precision}, ec);
        assertEquals(0, ts.getNanos() % division);
    }
}