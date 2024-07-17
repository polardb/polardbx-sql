package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import org.junit.Test;

public class MySQLUnicodeUtilsTest {

    @Test
    public void testUtf8ToLatin1() {
        byte[] latin1 = {-128, 0, 0, 0, 0, 0, 0, 3, 0};
        int len = 9;
        byte[] res = new byte[len];
        Assert.assertTrue(!MySQLUnicodeUtils.utf8ToLatin1(latin1, 0, len, res));
        byte[] utf8 = MySQLUnicodeUtils.latin1ToUtf8(latin1).getBytes();
        Assert.assertTrue(MySQLUnicodeUtils.utf8ToLatin1(utf8, 0, len, res));
    }

}
