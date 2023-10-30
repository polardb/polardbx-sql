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

package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 0x0 - 0x7f | 0xxxxxxx
 * 0x80 - 0x7ff | 110xxxxx 10xxxxxx
 * 0x800 - 0xffff | 1110xxxx 10xxxxxx 10xxxxxx
 * 0x10000 - 0x10ffff | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
 */
public class Utf8mb4GeneralCiTest {
    private static final int STRING_SIZE = 1 << 16;

    /**
     * The hex data in utf8mb4.yml is the source array to be sort.
     * The hex data int utf8mb4_general_ci is the result of SQL:
     * select hex(v_utf8mb4_general_ci)
     * from collation_test
     * order by v_utf8mb4_general_ci, hex(v_utf8mb4_general_ci)
     */
    @Test
    public void testOrder() {
        Yaml yaml = new Yaml();
        String[] source = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4.yml"), String[].class);
        String[] target = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4_general_ci.result.yml"),
            String[].class);

        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        CollationHandler collationHandler = charsetHandler.getCollationHandler();

        List<Slice> sliceList = new ArrayList<>();

        Arrays.stream(source)
            .map(Utf8mb4GeneralCiTest::hexStringToByteArray)
            .map(Slices::wrappedBuffer)
            .forEach(sliceList::add);

        // sort the array of utf8mb4 characters.
        int[] index = new int[sliceList.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }

        Integer[] sorted = Arrays.stream(index).boxed()
            .sorted((i, i1) -> {
                Slice left = sliceList.get(i);
                Slice right = sliceList.get(i1);
                int x = collationHandler.compareSp(left, right);
                if (x == 0) {
                    String h1 = bytesToHex(left.getBytes());
                    String h2 = bytesToHex(right.getBytes());
                    return h1.compareTo(h2);
                }
                return x;
            }).toArray(Integer[]::new);

        // compare the result of MySQL and PolarDB-X
        List<String> sortedHexes = Arrays.stream(sorted)
            .map(sliceList::get)
            .map(Slice::getBytes)
            .map(Utf8mb4GeneralCiTest::bytesToHex)
            .collect(Collectors.toList());

        Assert.assertEquals(sortedHexes.size(), target.length);
        for (int i = 0; i < target.length; i++) {
            Assert.assertTrue(String.format("i = %s, expect = %s, actual = %s", i, target[i], sortedHexes.get(i)),
                target[i].equalsIgnoreCase(sortedHexes.get(i)));
        }
    }

    @Test
    public void testSortKey() {
        Yaml yaml = new Yaml();
        String[] source = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4.yml"), String[].class);
        String[] target = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4_general_ci.result.yml"),
            String[].class);

        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        CollationHandler collationHandler = charsetHandler.getCollationHandler();

        List<Slice> sliceList = new ArrayList<>();

        Arrays.stream(source)
            .map(Utf8mb4GeneralCiTest::hexStringToByteArray)
            .map(Slices::wrappedBuffer)
            .forEach(sliceList::add);

        // sort the array of utf8mb4 characters.
        int[] index = new int[sliceList.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }

        Integer[] sorted = Arrays.stream(index).boxed()
            .sorted((i, i1) -> {
                Slice left = sliceList.get(i);
                Slice right = sliceList.get(i1);
                SortKey sortKey1 = collationHandler.getSortKey(left, 128);
                SortKey sortKey2 = collationHandler.getSortKey(right, 128);
                int x = sortKey1.compareTo(sortKey2);
                if (x == 0) {
                    String h1 = bytesToHex(left.getBytes());
                    String h2 = bytesToHex(right.getBytes());
                    return h1.compareTo(h2);
                }
                return x;
            }).toArray(Integer[]::new);

        // compare the result of MySQL and PolarDB-X
        List<String> sortedHexes = Arrays.stream(sorted)
            .map(sliceList::get)
            .map(Slice::getBytes)
            .map(Utf8mb4GeneralCiTest::bytesToHex)
            .collect(Collectors.toList());

        Assert.assertEquals(sortedHexes.size(), target.length);
        for (int i = 0; i < target.length; i++) {
            Assert.assertTrue(String.format("i = %s, expect = %s, actual = %s", i, target[i], sortedHexes.get(i)),
                target[i].equalsIgnoreCase(sortedHexes.get(i)));
        }
    }

    /**
     * Check the equals consistency: strings that are equal to each other must return the same hashCode
     */
    @Test
    public void testHashCode() {
        Yaml yaml = new Yaml();
        String[] source = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4.yml"), String[].class);

        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8MB4, CollationName.UTF8MB4_GENERAL_CI);
        CollationHandler collationHandler = charsetHandler.getCollationHandler();

        List<Slice> sliceList = new ArrayList<>();

        Arrays.stream(source)
            .map(Utf8mb4GeneralCiTest::hexStringToByteArray)
            .map(Slices::wrappedBuffer)
            .forEach(sliceList::add);

        // sort the array of utf8mb4 characters.
        int[] index = new int[sliceList.size()];
        for (int i = 0; i < index.length; i++) {
            index[i] = i;
        }

        Integer[] sorted = Arrays.stream(index).boxed().sorted(
            (i, i1) -> {
                Slice left = sliceList.get(i);
                Slice right = sliceList.get(i1);
                int x = collationHandler.compareSp(left, right);
                if (x == 0) {
                    String h1 = bytesToHex(left.getBytes());
                    String h2 = bytesToHex(right.getBytes());
                    return h1.compareTo(h2);
                }
                return x;
            }
        ).toArray(Integer[]::new);

        Arrays.stream(sorted)
            .map(sliceList::get)
            .reduce(
                (s1, s2) -> {
                    if (collationHandler.compareSp(s1, s2) == 0) {
                        int h1 = collationHandler.hashcode(s1);
                        int h2 = collationHandler.hashcode(s2);
                        Assert.assertTrue("Violating the Equals Consistency:"
                                + " s1 = " + bytesToHex(s1.getBytes()) + " h1 = " + h1 + "\n"
                                + ", s2 = " + bytesToHex(s2.getBytes()) + " h2 = " + h2,
                            h1 == h2);
                    }
                    return s2;
                }
            );
    }

    public static byte[] codePointsToBytes(int codepoint) {
        Charset charset = Charset.forName("UTF-8");
        return Optional.of(codepoint)
            .map(Character::toChars)
            .map(String::new)
            .map(s -> s.getBytes(charset))
            .orElseThrow(
                () -> new RuntimeException("invalid codepoint")
            );
    }

    /**
     * Generate random utf8mb4 hex data.
     */
    private static void generateStrings() throws IOException {
        Random random = new Random();
        String[] codepoints = new String[STRING_SIZE];
        // 1-5 characters
        for (int i = 0; i < STRING_SIZE; i++) {
            int characters = (Math.abs(random.nextInt()) % 5) + 1;
            Preconditions.checkArgument(1 <= characters && characters <= 5);

            ByteBuffer buff = ByteBuffer.allocate(4 * characters + 10);
            for (int j = 0; j < characters; j++) {
                // 1/5 prob
                int codepoint = random.nextInt(5) == 1 ? random.nextInt(0x10ffff) : random.nextInt(0x7f);
                byte[] bytes = codePointsToBytes(codepoint);
                buff.put(bytes);
            }
            buff.flip();
            byte[] utf8str = new byte[buff.remaining()];
            buff.get(utf8str);
            codepoints[i] = bytesToHex(utf8str);
        }
        Yaml yaml = new Yaml();
        yaml.dump(codepoints,
            new FileWriter(Utf8mb4GeneralCiTest.class.getResource("utf8mb4.yml").getFile()));
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Test
    public void test() {
        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8, CollationName.UTF8_BIN);
        CollationHandler collationHandler = charsetHandler.getCollationHandler();
        Slice left = Slices.wrappedBuffer(hexStringToByteArray("4A"));
        Slice right = Slices.wrappedBuffer(hexStringToByteArray("6A"));

        SortKey sortKey1 = collationHandler.getSortKey(left, 128);
        SortKey sortKey2 = collationHandler.getSortKey(right, 128);

        System.out.println(collationHandler.compareSp(left, right));
        System.out.println(sortKey1.compareTo(sortKey2));
    }
}
