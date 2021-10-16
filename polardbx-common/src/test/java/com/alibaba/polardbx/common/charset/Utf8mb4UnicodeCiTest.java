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

package com.alibaba.polardbx.common.charset;

import com.alibaba.polardbx.common.collation.CollationHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Utf8mb4UnicodeCiTest {
    /**
     * The hex data in utf8mb4.yml is the source array to be sort.
     * The hex data int utf8mb4_unicode_ci.result.yml is the result of SQL:
     * select hex(v_utf8mb4_unicode_ci)
     * from collation_test
     * order by v_utf8mb4_unicode_ci, hex(v_utf8mb4_unicode_ci)
     */
    @Test
    public void testOrder() {
        Yaml yaml = new Yaml();
        String[] source = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4.yml"), String[].class);
        String[] target = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4_unicode_ci.result.yml"),
            String[].class);

        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
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
                    String h1 = Utf8mb4GeneralCiTest.bytesToHex(left.getBytes());
                    String h2 = Utf8mb4GeneralCiTest.bytesToHex(right.getBytes());
                    return h1.compareTo(h2);
                }
                return x;
            }
        ).toArray(Integer[]::new);

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
        String[] target = yaml.loadAs(Utf8mb4GeneralCiTest.class.getResourceAsStream("utf8mb4_unicode_ci.result.yml"),
            String[].class);

        CharsetHandler charsetHandler =
            CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.UTF8MB4, CollationName.UTF8MB4_UNICODE_CI);
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

        Integer[] sorted = Arrays.stream(index)
            .boxed()
            .sorted((i, i1) -> {
                Slice left = sliceList.get(i);
                Slice right = sliceList.get(i1);
                SortKey sortKey1 = collationHandler.getSortKey(left, 128);
                SortKey sortKey2 = collationHandler.getSortKey(right, 128);
                int x = sortKey1.compareTo(sortKey2);
                if (x == 0) {
                    String h1 = Utf8mb4GeneralCiTest.bytesToHex(left.getBytes());
                    String h2 = Utf8mb4GeneralCiTest.bytesToHex(right.getBytes());
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
}
