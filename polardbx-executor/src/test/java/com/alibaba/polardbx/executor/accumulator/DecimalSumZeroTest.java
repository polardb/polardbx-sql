package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class DecimalSumZeroTest {
    private final int count;
    private DecimalBlock decimalBlock;
    private BigDecimal targetSum;

    public DecimalSumZeroTest(int count) {
        this.count = count;
    }

    @Parameterized.Parameters(name = "count={0}")
    public static Collection<Object[]> prepare() {
        int[] counts = {10, 500, 1000, 2000, 3000, 5000};
        List<Object[]> res = new ArrayList<>();
        for (int count : counts) {
            res.add(new Object[] {count});
        }
        return res;
    }

    private static DecimalBlock buildDecimalBlock(Decimal[] decimals) {
        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(4, new DecimalType(20, 2));
        for (Decimal decimal : decimals) {
            decimalBlockBuilder.writeDecimal(decimal);
        }
        return (DecimalBlock) decimalBlockBuilder.build();
    }

    @Before
    public void setUp() {
        Random random = new Random(System.currentTimeMillis());
        BigDecimal targetSumDecimal = BigDecimal.ZERO;

        // all decimal values are negative or positive of the same absolute value.
        String intPart = getIntPart(random);
        String fracPart = getFracPart(random);
        String decStr = String.format("%s.%s", intPart, fracPart);
        String negDecStr = '-' + decStr;

        Decimal[] decimalArr = new Decimal[count];
        for (int i = 0; i < count; i++) {
            String strVal = random.nextInt() % 2 == 0 ? decStr : negDecStr;
            decimalArr[i] = Decimal.fromString(strVal);
            targetSumDecimal = targetSumDecimal.add(new BigDecimal(strVal));
        }
        decimalBlock = buildDecimalBlock(decimalArr);

        targetSum = targetSumDecimal;
    }

    private String getIntPart(Random random) {
        return String.valueOf(random.nextInt(100_000_000));
    }

    private String getFracPart(Random random) {
        return String.valueOf(random.nextInt(100_000_000));
    }

    @Test
    public void testDecimalSum() {

        Decimal cache = new Decimal();
        Decimal afterValue = new Decimal();

        for (int pos = 0; pos < decimalBlock.getPositionCount(); pos++) {
            Decimal beforeValue = afterValue;
            Decimal value = decimalBlock.getDecimal(pos);

            // avoid reset memory to 0
            FastDecimalUtils.add(
                beforeValue.getDecimalStructure(),
                value.getDecimalStructure(),
                cache.getDecimalStructure(),
                false);

            // swap variants to avoid allocating memory
            afterValue = cache;
            cache = beforeValue;
        }

        Decimal sum = afterValue;
        Assert.assertEquals(0, targetSum.compareTo(new BigDecimal(sum.toString())));
    }
}
