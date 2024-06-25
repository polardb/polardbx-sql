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

import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class WildCardTest {
    private static final CollationHandler COLLATION_HANDLER =
        CharsetFactory.DEFAULT_CHARSET_HANDLER.getCollationHandler();
    private static final Slice CHECK_STR =
        Slices.utf8Slice(

            "CAAAACCACTATGAGATATCATCTCACACCAGTTAGAATGGCAATCATTAAAAAGTCAGGAAACAACAGGTGCTGGAGAGGATGCGGAGAAATAGGAACAC");
    private static final String[] MATCHED = {
        "%CAAAACCACTATGAGATATCATCTCACACCAGTTA%",
        "%AAAACCACTATGAGATATCATCTCACACCAGTTAG%",
        "%AAAACCACTATGAGATATCATCTCACACCAGTTAG%",
        "%AAAACCACTATGAGATATCATCTCACACCAGTTAG%",
        "%AAAACCACTATGAGATATCATCTCACACCAGTTAG%",
        "%CAAAACCACTATGAGATATCATCTCACACCAGTTA%",
        "%CAAAACCACTATGAGATATCATCTCACACCAGTTA%",
        "%AAAACCACTATGAGATATCATCTCACACCAGTTAG%",
        "%AAACCACTATGAGATATCATCTCACACCAGTTAGA%",
        "%AACCACTATGAGATATCATCTCACACCAGTTAGAA%",
        "%ACCACTATGAGATATCATCTCACACCAGTTAGAAT%",
        "%CCACTATGAGATATCATCTCACACCAGTTAGAATG%",
        "%CACTATGAGATATCATCTCACACCAGTTAGAATGG%",
        "%ACTATGAGATATCATCTCACACCAGTTAGAATGGC%",
        "%CTATGAGATATCATCTCACACCAGTTAGAATGGCA%",
        "%TATGAGATATCATCTCACACCAGTTAGAATGGCAA%",
        "%ATGAGATATCATCTCACACCAGTTAGAATGGCAAT%",
        "%TGAGATATCATCTCACACCAGTTAGAATGGCAATC%",
        "%GAGATATCATCTCACACCAGTTAGAATGGCAATCA%",
        "%AGATATCATCTCACACCAGTTAGAATGGCAATCAT%",
        "%GATATCATCTCACACCAGTTAGAATGGCAATCATT%",
        "%ATATCATCTCACACCAGTTAGAATGGCAATCATTA%",
        "%TATCATCTCACACCAGTTAGAATGGCAATCATTAA%",
        "%ATCATCTCACACCAGTTAGAATGGCAATCATTAAA%",
        "%TCATCTCACACCAGTTAGAATGGCAATCATTAAAA%",
        "%CATCTCACACCAGTTAGAATGGCAATCATTAAAAA%",
        "%ATCTCACACCAGTTAGAATGGCAATCATTAAAAAG%",
        "%TCTCACACCAGTTAGAATGGCAATCATTAAAAAGT%",
        "%CTCACACCAGTTAGAATGGCAATCATTAAAAAGTC%",
        "%TCACACCAGTTAGAATGGCAATCATTAAAAAGTCA%",
        "%CACACCAGTTAGAATGGCAATCATTAAAAAGTCAG%",
        "%ACACCAGTTAGAATGGCAATCATTAAAAAGTCAGG%",
        "%CACCAGTTAGAATGGCAATCATTAAAAAGTCAGGA%",
        "%ACCAGTTAGAATGGCAATCATTAAAAAGTCAGGAA%",
        "%CCAGTTAGAATGGCAATCATTAAAAAGTCAGGAAA%",
        "%CAGTTAGAATGGCAATCATTAAAAAGTCAGGAAAC%",
        "%AGTTAGAATGGCAATCATTAAAAAGTCAGGAAACA%",
        "%GTTAGAATGGCAATCATTAAAAAGTCAGGAAACAA%",
        "%TTAGAATGGCAATCATTAAAAAGTCAGGAAACAAC%",
        "%TAGAATGGCAATCATTAAAAAGTCAGGAAACAACA%",
        "%AGAATGGCAATCATTAAAAAGTCAGGAAACAACAG%",
        "%GAATGGCAATCATTAAAAAGTCAGGAAACAACAGG%",
        "%AATGGCAATCATTAAAAAGTCAGGAAACAACAGGT%",
        "%ATGGCAATCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%TGGCAATCATTAAAAAGTCAGGAAACAACAGGTGC%",
        "%GCAATCATTAAAAAGTCAGGAAACAACAGGTGCTG%",
        "%CAATCATTAAAAAGTCAGGAAACAACAGGTGCTGG%",
        "%AATCATTAAAAAGTCAGGAAACAACAGGTGCTGGA%",
        "%ATCATTAAAAAGTCAGGAAACAACAGGTGCTGGAG%",
        "%TCATTAAAAAGTCAGGAAACAACAGGTGCTGGAGA%",
        "%CATTAAAAAGTCAGGAAACAACAGGTGCTGGAGAG%",
        "%ATTAAAAAGTCAGGAAACAACAGGTGCTGGAGAGG%",
        "%TTAAAAAGTCAGGAAACAACAGGTGCTGGAGAGGA%",
        "%TAAAAAGTCAGGAAACAACAGGTGCTGGAGAGGAT%",
        "%AAAAAGTCAGGAAACAACAGGTGCTGGAGAGGATG%",
        "%AAAAGTCAGGAAACAACAGGTGCTGGAGAGGATGC%",
        "%AAAGTCAGGAAACAACAGGTGCTGGAGAGGATGCG%",
        "%AAGTCAGGAAACAACAGGTGCTGGAGAGGATGCGG%",
        "%AGTCAGGAAACAACAGGTGCTGGAGAGGATGCGGA%",
        "%GTCAGGAAACAACAGGTGCTGGAGAGGATGCGGAG%",
        "%TCAGGAAACAACAGGTGCTGGAGAGGATGCGGAGA%",
        "%CAGGAAACAACAGGTGCTGGAGAGGATGCGGAGAA%",
        "%AGGAAACAACAGGTGCTGGAGAGGATGCGGAGAAA%",
        "%GGAAACAACAGGTGCTGGAGAGGATGCGGAGAAAT%",
        "%GAAACAACAGGTGCTGGAGAGGATGCGGAGAAATA%",
        "%AAACAACAGGTGCTGGAGAGGATGCGGAGAAATAG%",
        "%AACAACAGGTGCTGGAGAGGATGCGGAGAAATAGG%",
        "%ACAACAGGTGCTGGAGAGGATGCGGAGAAATAGGA%",
        "%CAACAGGTGCTGGAGAGGATGCGGAGAAATAGGAA%",
        "%AACAGGTGCTGGAGAGGATGCGGAGAAATAGGAAC%",
        "%ACAGGTGCTGGAGAGGATGCGGAGAAATAGGAACA%",
        "%CAGGTGCTGGAGAGGATGCGGAGAAATAGGAACAC%",
        "%",
        "_%_",
        "%_%",
        "%CAGGTG%GAACA_%",
    };

    private static final String[] UNMATCHED = {
        "%ATTGACCACACTCTACTATAGAGTATCACCAAAAC%",
        "%GATTGACCACACTCTACTATAGAGTATCACCAAAA%",
        "%AGATTGACCACACTCTACTATAGAGTATCACCAAA%",
        "%AAGATTGACCACACTCTACTATAGAGTATCACCAA%",
        "%TAAGATTGACCACACTCTACTATAGAGTATCACCA%",
        "%GTAAGATTGACCACACTCTACTATAGAGTATCACC%",
        "%GGTAAGATTGACCACACTCTACTATAGAGTATCAC%",
        "%CGGTAAGATTGACCACACTCTACTATAGAGTATCA%",
        "%ACGGTAAGATTGACCACACTCTACTATAGAGTATC%",
        "%AACGGTAAGATTGACCACACTCTACTATAGAGTAT%",
        "%TAACGGTAAGATTGACCACACTCTACTATAGAGTA%",
        "%CTAACGGTAAGATTGACCACACTCTACTATAGAGT%",
        "%ACTAACGGTAAGATTGACCACACTCTACTATAGAG%",
        "%TACTAACGGTAAGATTGACCACACTCTACTATAGA%",
        "%TTACTAACGGTAAGATTGACCACACTCTACTATAG%",
        "%ATTACTAACGGTAAGATTGACCACACTCTACTATA%",
        "%AATTACTAACGGTAAGATTGACCACACTCTACTAT%",
        "%AAATTACTAACGGTAAGATTGACCACACTCTACTA%",
        "%AAAATTACTAACGGTAAGATTGACCACACTCTACT%",
        "%AAAAATTACTAACGGTAAGATTGACCACACTCTAC%",
        "%GAAAAATTACTAACGGTAAGATTGACCACACTCTA%",
        "%TGAAAAATTACTAACGGTAAGATTGACCACACTCT%",
        "%CTGAAAAATTACTAACGGTAAGATTGACCACACTC%",
        "%ACTGAAAAATTACTAACGGTAAGATTGACCACACT%",
        "%GACTGAAAAATTACTAACGGTAAGATTGACCACAC%",
        "%GGACTGAAAAATTACTAACGGTAAGATTGACCACA%",
        "%AGGACTGAAAAATTACTAACGGTAAGATTGACCAC%",
        "%AAGGACTGAAAAATTACTAACGGTAAGATTGACCA%",
        "%AAAGGACTGAAAAATTACTAACGGTAAGATTGACC%",
        "%CAAAGGACTGAAAAATTACTAACGGTAAGATTGAC%",
        "%ACAAAGGACTGAAAAATTACTAACGGTAAGATTGA%",
        "%AACAAAGGACTGAAAAATTACTAACGGTAAGATTG%",
        "%CAACAAAGGACTGAAAAATTACTAACGGTAAGATT%",
        "%ACAACAAAGGACTGAAAAATTACTAACGGTAAGAT%",
        "%GACAACAAAGGACTGAAAAATTACTAACGGTAAGA%",
        "%GGACAACAAAGGACTGAAAAATTACTAACGGTAAG%",
        "%TGGACAACAAAGGACTGAAAAATTACTAACGGTAA%",
        "%GTGGACAACAAAGGACTGAAAAATTACTAACGGTA%",
        "%CGTGGACAACAAAGGACTGAAAAATTACTAACGGT%",
        "%TCGTGGACAACAAAGGACTGAAAAATTACTAACGG%",
        "%GTCGTGGACAACAAAGGACTGAAAAATTACTAACG%",
        "%GGTCGTGGACAACAAAGGACTGAAAAATTACTAAC%",
        "%AGGTCGTGGACAACAAAGGACTGAAAAATTACTAA%",
        "%GAGGTCGTGGACAACAAAGGACTGAAAAATTACTA%",
        "%AGAGGTCGTGGACAACAAAGGACTGAAAAATTACT%",
        "%GAGAGGTCGTGGACAACAAAGGACTGAAAAATTAC%",
        "%GGAGAGGTCGTGGACAACAAAGGACTGAAAAATTA%",
        "%AGGAGAGGTCGTGGACAACAAAGGACTGAAAAATT%",
        "%TAGGAGAGGTCGTGGACAACAAAGGACTGAAAAAT%",
        "%GTAGGAGAGGTCGTGGACAACAAAGGACTGAAAAA%",
        "%CGTAGGAGAGGTCGTGGACAACAAAGGACTGAAAA%",
        "%GCGTAGGAGAGGTCGTGGACAACAAAGGACTGAAA%",
        "%GGCGTAGGAGAGGTCGTGGACAACAAAGGACTGAA%",
        "%AGGCGTAGGAGAGGTCGTGGACAACAAAGGACTGA%",
        "%GAGGCGTAGGAGAGGTCGTGGACAACAAAGGACTG%",
        "%AGAGGCGTAGGAGAGGTCGTGGACAACAAAGGACT%",
        "%AAGAGGCGTAGGAGAGGTCGTGGACAACAAAGGAC%",
        "%AAAGAGGCGTAGGAGAGGTCGTGGACAACAAAGGA%",
        "%TAAAGAGGCGTAGGAGAGGTCGTGGACAACAAAGG%",
        "%ATAAAGAGGCGTAGGAGAGGTCGTGGACAACAAAG%",
        "%GATAAAGAGGCGTAGGAGAGGTCGTGGACAACAAA%",
        "%GGATAAAGAGGCGTAGGAGAGGTCGTGGACAACAA%",
        "%AGGATAAAGAGGCGTAGGAGAGGTCGTGGACAACA%",
        "%AAGGATAAAGAGGCGTAGGAGAGGTCGTGGACAAC%",
        "%CAAGGATAAAGAGGCGTAGGAGAGGTCGTGGACAA%",
        "%ACAAGGATAAAGAGGCGTAGGAGAGGTCGTGGACA%",
        "%CACAAGGATAAAGAGGCGTAGGAGAGGTCGTGGAC%"
    };

    @Test
    public void testUtf8GeneralCi() {
        CollationHandler collationHandler = CharsetFactory.DEFAULT_CHARSET_HANDLER.getCollationHandler();
        Assert.assertEquals(collationHandler, CollationHandlers.COLLATION_HANDLER_UTF8_GENERAL_CI);
        for (String matched : MATCHED) {
            Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(matched)));
        }

        for (String unmatched : UNMATCHED) {
            Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(unmatched)));
        }
    }

    @Test
    public void testLatin1Bin() {
        CollationHandler collationHandler = CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
        for (String matched : MATCHED) {
            Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(matched)));
        }

        for (String unmatched : UNMATCHED) {
            Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(unmatched)));
        }
    }

    @Test
    public void testLatin1BinContains() {
        CollationHandler collationHandler = CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
        for (String matched : MATCHED) {
            if (isContains(matched)) {
                String pattern = matched.substring(1, matched.length() - 1);
                int[] lps = computeLPSArray(Slices.utf8Slice(pattern).getBytes());
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.containsCompare(CHECK_STR, pattern.getBytes(), lps));
            } else {
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(matched)));
            }
        }

        for (String unmatched : UNMATCHED) {
            if (isContains(unmatched)) {
                String pattern = unmatched.substring(1, unmatched.length() - 1);
                int[] lps = computeLPSArray(Slices.utf8Slice(pattern).getBytes());
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.containsCompare(CHECK_STR, pattern.getBytes(), lps));
            } else {
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(unmatched)));
            }
        }
    }

    public static int[] computeLPSArray(byte[] pattern) {
        int[] lps = new int[pattern.length];
        int length = 0;
        lps[0] = 0;
        int i = 1;

        while (i < pattern.length) {
            if (pattern[i] == pattern[length]) {
                length++;
                lps[i] = length;
                i++;
            } else {
                if (length != 0) {
                    length = lps[length - 1];
                } else {
                    lps[i] = length;
                    i++;
                }
            }
        }
        return lps;
    }

    private boolean isContains(String pattern) {
        if (pattern == null || pattern.length() < 2) {
            return false;
        }
        byte[] bytes = pattern.getBytes();
        if (bytes[0] == CollationHandler.WILD_MANY && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ in the middle
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Gbk bin 没有实现wildcompare
     * 预期输出全为false
     */
    @Test
    public void testGbkBin() {
        CollationHandler collationHandler = CollationHandlers.COLLATION_HANDLER_GBK_BIN;
        for (String matched : MATCHED) {
            Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(matched)));
        }

        for (String unmatched : UNMATCHED) {
            Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(unmatched)));
        }
    }
}
