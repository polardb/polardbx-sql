package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;

// Test Case from MySQL 8.0 resource code: mysql-test/t/ctype_unicode900_as_cs.test
//  Test the characters in different groups are reordered correctly. For example,
//  U+33E8 is in the core group, and U+2F17 is in the Han group, and 'A' is in
//  the latin group. According to the reorder rule defined by the CLDR for the
//  Chinese collation, we should get U+33E8 < U+2F17 < 'A'. This also tests how
//  different glyphs of one Han character sort according to the weight shift rule
//  defined by CLDR. For example, U+3197 (IDEOGRAPHIC ANNOTATION MIDDLE MARK) and
//  U+4E2D (CJK UNIFIED IDEOGRAPH-4E2D) are different glyphs of a Chinese
//  character which means 'middle' and the CLDR defines "U+412D <<< U+3197".
public class Utf8mb4Zh0900AsCsCompareTest {
    @Test
    public void test1() {
        String[] testStrList = {
            "⺇", "⺍", "⼗", "〸", "Ⓐ", "中", "㆗", "A", "a", "Z", "z", "も", "モ", "⸱", "㏨",
            "節"};

        String[] expectedOrderList = {
            "⸱", "㏨", "⺇", "節", "⼗", "〸", "⺍", "中", "㆗", "a", "A", "Ⓐ", "z", "Z", "も",
            "モ"};

        CollationHandler collationHandler =
            CharsetFactory.INSTANCE.createCollationHandler(CollationName.UTF8MB4_ZH_0900_AS_CS);

        int[] indexes = new int[testStrList.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }

        IntArrays.quickSort(indexes, new AbstractIntComparator() {
            @Override
            public int compare(int i, int i1) {

                Slice left = Slices.utf8Slice(testStrList[i]);
                Slice right = Slices.utf8Slice(testStrList[i1]);

                return collationHandler.compareSp(left, right);
            }
        });

        try {
            for (int i = 0; i < indexes.length; i++) {
                Assert.assertTrue(expectedOrderList[i].equals(testStrList[indexes[i]]));
            }
        } catch (AssertionError e) {
            System.out.println("expected: ");
            System.out.println(Arrays.toString(expectedOrderList));

            System.out.println("actual:");
            StringBuilder builder = new StringBuilder();
            Arrays.stream(indexes).forEach(i -> builder.append(testStrList[i]).append(", "));
            System.out.println(builder);

            throw e;
        }

    }

    @Test
    public void test2() {
        doTest(
            new Slice[] {
                Slices.wrappedBuffer(hexToArray("E2BA87")),
                Slices.wrappedBuffer(hexToArray("E2BA8D")),
                Slices.wrappedBuffer(hexToArray("E2BC97")),
                Slices.wrappedBuffer(hexToArray("E380B8")),
                Slices.wrappedBuffer(hexToArray("E292B6")),
                Slices.wrappedBuffer(hexToArray("F09F8590")),
                Slices.wrappedBuffer(hexToArray("E4B8AD")),
                Slices.wrappedBuffer(hexToArray("E38697")),
                Slices.wrappedBuffer(hexToArray("F09F88AD")),
                Slices.wrappedBuffer(hexToArray("41")),
                Slices.wrappedBuffer(hexToArray("61")),
                Slices.wrappedBuffer(hexToArray("5A")),
                Slices.wrappedBuffer(hexToArray("7A")),
                Slices.wrappedBuffer(hexToArray("E38282")),
                Slices.wrappedBuffer(hexToArray("E383A2")),
                Slices.wrappedBuffer(hexToArray("E2B8B1")),
                Slices.wrappedBuffer(hexToArray("E38FA8")),
                Slices.wrappedBuffer(hexToArray("F09F88A9")),
                Slices.wrappedBuffer(hexToArray("F09F8981")),
                Slices.wrappedBuffer(hexToArray("EFA996"))
            },
            new Slice[] {
                Slices.wrappedBuffer(hexToArray("E2B8B1")),
                Slices.wrappedBuffer(hexToArray("F09F8981")),
                Slices.wrappedBuffer(hexToArray("E38FA8")),
                Slices.wrappedBuffer(hexToArray("E2BA87")),
                Slices.wrappedBuffer(hexToArray("EFA996")),
                Slices.wrappedBuffer(hexToArray("E2BC97")),
                Slices.wrappedBuffer(hexToArray("E380B8")),
                Slices.wrappedBuffer(hexToArray("E2BA8D")),
                Slices.wrappedBuffer(hexToArray("F09F88A9")),
                Slices.wrappedBuffer(hexToArray("E4B8AD")),
                Slices.wrappedBuffer(hexToArray("E38697")),
                Slices.wrappedBuffer(hexToArray("F09F88AD")),
                Slices.wrappedBuffer(hexToArray("61")),
                Slices.wrappedBuffer(hexToArray("41")),
                Slices.wrappedBuffer(hexToArray("E292B6")),
                Slices.wrappedBuffer(hexToArray("F09F8590")),
                Slices.wrappedBuffer(hexToArray("7A")),
                Slices.wrappedBuffer(hexToArray("5A")),
                Slices.wrappedBuffer(hexToArray("E38282")),
                Slices.wrappedBuffer(hexToArray("E383A2"))
            },
            t -> t,
            Slice::compareTo,
            Slice::toStringUtf8
        );
    }

    public static <T> void doTest(T[] testStrList, T[] expectedOrderList,
                                  Function<T, Slice> toSliceFunc,
                                  Comparator<T> comparator,
                                  Function<T, String> toStringFunc
    ) {
        Preconditions.checkArgument(expectedOrderList.length == testStrList.length);
        CollationHandler collationHandler =
            CharsetFactory.INSTANCE.createCollationHandler(CollationName.UTF8MB4_ZH_0900_AS_CS);

        int[] indexes = new int[testStrList.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = i;
        }

        IntArrays.quickSort(indexes, new AbstractIntComparator() {
            @Override
            public int compare(int i, int i1) {

                Slice left = toSliceFunc.apply(testStrList[i]);
                Slice right = toSliceFunc.apply(testStrList[i1]);

                int res = collationHandler.compareSp(left, right);
                if (res == 0) {
                    return left.compareTo(right);
                }
                return res;
            }
        });

        try {
            for (int i = 0; i < indexes.length; i++) {
                Assert.assertTrue(
                    comparator.compare(expectedOrderList[i], testStrList[indexes[i]]) == 0
                );
            }
        } catch (AssertionError e) {
            System.out.println("expected: ");
            System.out.println(toString(expectedOrderList, toStringFunc));

            System.out.println("actual:");
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            Arrays.stream(indexes).forEach(i -> builder.append(toStringFunc.apply(testStrList[i])).append(", "));
            builder.deleteCharAt(builder.length() - 1);
            builder.append(']');
            System.out.println(builder);

            throw e;
        }
    }

    public static byte[] hexToArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static <T> String toString(T[] a, Function<T, String> toStringFunc) {
        if (a == null) {
            return "null";
        }

        int iMax = a.length - 1;
        if (iMax == -1) {
            return "[]";
        }

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(toStringFunc.apply(a[i]));
            if (i == iMax) {
                return b.append(']').toString();
            }
            b.append(", ");
        }
    }
}
