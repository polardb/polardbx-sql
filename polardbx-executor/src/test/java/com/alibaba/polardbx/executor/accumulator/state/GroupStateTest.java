package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.common.datatype.Decimal;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupStateTest {

    private final Random random = new Random();

    @Test
    public void testNullableDecimal() {
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

    @Test
    public void testLong() {
        final int COUNT = 1000;
        long[] values = new long[COUNT];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextLong();
        }

        LongGroupState state = new LongGroupState(COUNT / 10);
        for (int i = 0; i < values.length; i++) {
            state.append(values[i]);
        }

        for (int i = 0; i < values.length / 2; i += 2) {
            long l = random.nextLong();
            state.set(i, l);
            values[i] = l;
        }

        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], state.get(i));
        }
    }

    @Test
    public void testNullableDouble() {
        final int COUNT = 1000;
        Double[] values = new Double[COUNT];
        for (int i = 0; i < values.length; i++) {
            if (i % 2 == 0) {
                values[i] = null;
            } else {
                values[i] = random.nextDouble();
            }
        }

        NullableDoubleGroupState state = new NullableDoubleGroupState(100);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                state.append(values[i]);
            } else {
                state.appendNull();
            }
        }

        for (int i = 0; i < values.length / 2; i += 2) {
            double d = random.nextDouble();
            state.set(i, d);
            values[i] = d;
        }

        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(state.isNull(i));
                assertEquals(values[i], state.get(i), 1e-10);
            } else {
                assertTrue(state.isNull(i));
            }
        }
    }
}
