package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
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
public class DecimalBoxTest {

    private static final int scale = 9;
    private final int count;
    private final SimpleMode simpleMode;
    private DecimalBlock decimalBlock;
    private BigDecimal targetSum;

    public DecimalBoxTest(int count, String mode) {
        this.count = count;
        this.simpleMode = SimpleMode.parse(mode);
    }

    @Parameterized.Parameters(name = "count={0}, mode={1}")
    public static Collection<Object[]> prepare() {
        int[] counts = {1, 2, 3, 4, 8, 999, 1000, 1024, 1025};
        String[] modes = {"SIMPLE1", "SIMPLE2", "SIMPLE3"};
        List<Object[]> res = new ArrayList<>();
        for (int count : counts) {
            for (String mode : modes) {
                Object[] objects = new Object[2];
                objects[0] = count;
                objects[1] = mode;
                res.add(objects);
            }
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

        Decimal[] decimalArr = new Decimal[count];
        for (int i = 0; i < count; i++) {
            String intPart = getIntPart(random);
            String fracPart = getFracPart(random);
            String decStr = String.format("%s.%s", intPart, fracPart);

            decimalArr[i] = Decimal.fromString(decStr);
            targetSumDecimal = targetSumDecimal.add(new BigDecimal(decStr));
        }
        decimalBlock = buildDecimalBlock(decimalArr);

        targetSum = targetSumDecimal;
    }

    private String getIntPart(Random random) {
        switch (simpleMode) {
        case SIMPLE1:
            return "0";
        case SIMPLE2:
            return String.valueOf(random.nextInt(500_000_000) + 100_000_000);
        case SIMPLE3:
            String str1 = String.valueOf(random.nextInt(500_000_000) + 100_000_000);
            String str2 = String.valueOf(random.nextInt(500_000_000) + 100_000_000);
            return str1 + str2;
        }
        throw new UnsupportedOperationException(simpleMode.name());
    }

    private String getFracPart(Random random) {
        return String.valueOf(random.nextInt(800_000_000) + 100_000_000);
    }

    @Test
    public void testDecimalBoxSimple() {
        Assert.assertTrue(decimalBlock.isSimple());
        DecimalBox box = new DecimalBox(scale);
        for (int pos = 0; pos < decimalBlock.getPositionCount(); pos++) {

            int a1 = decimalBlock.fastInt1(pos);
            int a2 = decimalBlock.fastInt2(pos);
            int b = decimalBlock.fastFrac(pos);
            box.add(a1, a2, b);
        }
        Decimal sum = box.getDecimalSum();
        Assert.assertEquals(0, targetSum.compareTo(new BigDecimal(sum.toString())));
    }

    enum SimpleMode {
        SIMPLE1,    // frac * 10^-9
        SIMPLE2,    // int1 + frac * 10^-9
        SIMPLE3;    // int2 * 10^9 + int1 + frac * 10^-9

        static SimpleMode parse(String mode) {
            if (mode == null) {
                throw new UnsupportedOperationException();
            }
            switch (mode.toUpperCase()) {
            case "SIMPLE1":
                return SIMPLE1;
            case "SIMPLE2":
                return SIMPLE2;
            case "SIMPLE3":
                return SIMPLE3;
            }
            throw new UnsupportedOperationException(mode);
        }

    }
}
