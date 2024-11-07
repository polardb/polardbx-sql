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

    enum LikeType {
        PREFIX,
        SUFFIX,
        MIDDLE,
        OTHERS
    }

    private static final CollationHandler COLLATION_HANDLER =
        CharsetFactory.DEFAULT_CHARSET_HANDLER.getCollationHandler();
    private static final Slice CHECK_STR =
        Slices.utf8Slice(

            "CAAAACCACTATGAGATATCATCTCACACCAGTTAGAATGGCAATCATTAAAAAGTCAGGAAACAACAGGTGCTGGAGAGGATGCGGAGAAATAGGAACAC");
    private static final String[] MATCHED = {
        // MIDDLE_MATCHED
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
        "%TGGCAATCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%GCAATCATTAAAAAGTCAGGAAACAACAGGTGC%",
        "%CAATCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%AATCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%ATCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%TCATTAAAAAGTCAGGAAACAACAGGTG%",
        "%CATTAAAAAGTCAGGAAACAACAGGTG%",
        "%ATTAAAAAGTCAGGAAACAACAGGTG%",
        "%TTAAAAAGTCAGGAAACAACAGGTG%",
        "%TAAAAAGTCAGGAAACAACAGGTG%",
        "%AAAAAGTCAGGAAACAACAGGTG%",
        "%AAAAGTCAGGAAACAACAGGTG%",
        "%AAAGTCAGGAAACAACAGGTG%",
        "%AAGTCAGGAAACAACAGGTG%",
        "%AGTCAGGAAACAACAGGTG%",
        "%GTCAGGAAACAACAGGTGC%",
        "%TCAGGAAACAACAGGTGC%",
        "%CAGGAAACAACAGGTGC%",
        "%AGGAAACAACAGGTGC%",
        "%GGAAACAACAGGTGC%",
        "%GAAACAACAGG%",
        "%AAACAACAG%",
        "%AACAACAGG%",
        "%ACAACA%",
        "%CAACA%",
        "%AAC%",
        "%AC%",
        "%C%",

        // PREFIX_MATCHED
        "CA%",
        "CAA%",
        "CAAA%",
        "CAAAA%",
        "CAAAAC%",
        "CAAAACC%",
        "CAAAACCA%",
        "CAAAACCAC%",
        "CAAAACCACT%",
        "CAAAACCACTA%",

        // SUFFIX_MATCHED
        "%C",
        "%AC",
        "%CAC",
        "%ACAC",
        "%AACAC",
        "%GAACAC",
        "%GGAACAC",
        "%AGGAACAC",
        "%TAGGAACAC",
        "%ATAGGAACAC",

        // others
        "%",
        "%%",
        "%%%",
        "_%_",
        "%_%",
        "%__%",
        "%__%_%",
        "%CAGGTG%GAACA_%",
    };

    private static final String[] UNMATCHED = {
        // MIDDLE_UNMATCHED
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
        "%CACAAGGATAAAGAGGCGTAGGAGAGGTCGTGGAC%",

        // PREFIX_UNMATCHED
        "CT%",
        "CCA%",
        "CGAA%",
        "CACAA%",
        "CATAAC%",
        "CATAACC%",
        "CACAACCA%",
        "CAGAACCAC%",
        "CACAACCACT%",
        "CAATACCACTA%",

        // SUFFIX_UNMATCHED
        "%G",
        "%AG",
        "%CAG",
        "%ACGC",
        "%AGCAC",
        "%GCACAC",
        "%GCAACAC",
        "%ACGAACAC",
        "%TTGGAACAC",
        "%AGAGGAACAC",

        // others
        "_",
        "__",
        "___",
        "%CAG_G%GAACA_%",
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
        Latin1BinCollationHandler collationHandler =
            (Latin1BinCollationHandler) CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
        String pattern;
        for (String matched : MATCHED) {
            switch (getLikeType(matched)) {
            case PREFIX:
                pattern = matched.substring(0, matched.length() - 1);
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.startsWith(CHECK_STR, pattern.getBytes()));
                break;
            case SUFFIX:
                pattern = matched.substring(1);
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.endsWith(CHECK_STR, pattern.getBytes()));
                break;
            case MIDDLE:
                pattern = matched.substring(1, matched.length() - 1);
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.contains(CHECK_STR, pattern.getBytes()));
                break;
            case OTHERS:
                Assert.assertTrue(String.format("select \'%s\' like \'%s\';", CHECK_STR, matched),
                    collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(matched)));
                break;
            }
        }

        for (String unmatched : UNMATCHED) {
            switch (getLikeType(unmatched)) {
            case PREFIX:
                pattern = unmatched.substring(0, unmatched.length() - 1);
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.startsWith(CHECK_STR, pattern.getBytes()));
                break;
            case SUFFIX:
                pattern = unmatched.substring(1);
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.endsWith(CHECK_STR, pattern.getBytes()));
                break;
            case MIDDLE:
                pattern = unmatched.substring(1, unmatched.length() - 1);
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.contains(CHECK_STR, pattern.getBytes()));
                break;
            case OTHERS:
                Assert.assertFalse(String.format("select \'%s\' like \'%s\';", CHECK_STR, unmatched),
                    collationHandler.wildCompare(CHECK_STR, Slices.utf8Slice(unmatched)));
                break;
            }
        }
    }

    private LikeType getLikeType(String pattern) {
        if (pattern == null) {
            return LikeType.OTHERS;
        }
        byte[] bytes = pattern.getBytes();
        if (bytes.length >= 2 && bytes[0] == CollationHandler.WILD_MANY
            && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ in the middle
                    return LikeType.OTHERS;
                }
            }
            return LikeType.MIDDLE;
        }

        if (bytes.length >= 1 && bytes[0] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ after the first one
                    return LikeType.OTHERS;
                }
            }
            return LikeType.SUFFIX;
        }
        if (bytes.length >= 1 && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 0; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ before the last one
                    return LikeType.OTHERS;
                }
            }
            return LikeType.PREFIX;
        }
        return LikeType.OTHERS;
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
