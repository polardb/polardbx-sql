package com.alibaba.polardbx.optimizer.config.table.collation;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.polardbx.optimizer.config.table.collation.AbstractCollationHandler.*;

// 0x0 - 0x7f | 0xxxxxxx
// 0x80 - 0x7ff | 110xxxxx 10xxxxxx
// 0x800 - 0xffff | 1110xxxx 10xxxxxx 10xxxxxx
// 0x10000 - 0x10ffff | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
public class CodepointOfUTF8Test {
    private int[] parsedWildCharacter = new int[1];
    private Slice str;
    private int result;

    @Test
    public void testMB4() {
        // 0x10000 - 0x10ffff | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        str = Slices.wrappedBuffer((byte) 0xF1, (byte) 0xA7, (byte) 0x9F, (byte) 0x83);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, 4);

        str = Slices.wrappedBuffer((byte) 0xF1, (byte) 0xA7, (byte) 0x9F);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_TOOSMALL4);

        str = Slices.wrappedBuffer((byte) 0xF1, (byte) 0xA7, (byte) 0x7F, (byte) 0x83);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);

        // if (*pwc < 0x10000 || *pwc > 0x10ffff) return MY_CS_ILSEQ;
        str = Slices.wrappedBuffer((byte) 0xF0, (byte) 0x80, (byte) 0x87, (byte) 0xE9);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);
    }

    @Test
    public void testMB3() {
        // 0x800 - 0xffff | 1110xxxx 10xxxxxx 10xxxxxx
        str = Slices.wrappedBuffer((byte) 0xE4, (byte) 0xA5, (byte) 0xBF);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, 3);

        str = Slices.wrappedBuffer((byte) 0xE4, (byte) 0xA5);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_TOOSMALL3);

        str = Slices.wrappedBuffer((byte) 0xE4, (byte) 0xC5, (byte) 0xBF);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);

        // According to RFC 3629, UTF-8 should prohibit characters between
        // U+D800 and U+DFFF, which are reserved for surrogate pairs and do
        // not directly represent characters.
        str = Slices.wrappedBuffer((byte) 0xED, (byte) 0xF5, (byte) 0xED);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);
    }

    @Test
    public void testMB2() {
        // 0x80 - 0x7ff | 110xxxxx 10xxxxxx
        str = Slices.wrappedBuffer((byte) 0xD7, (byte) 0xA0);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, 2);

        // 0x80 - 0x7ff | 110xxxxx 10xxxxxx MY_CS_ILSEQ ERROR 1
        str = Slices.wrappedBuffer((byte) 0xC1, (byte) 0xA0);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);

        // 0x80 - 0x7ff | 110xxxxx 10xxxxxx MY_CS_TOOSMALL2 ERROR
        str = Slices.wrappedBuffer((byte) 0xC5);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_TOOSMALL2);

        // 0x80 - 0x7ff | 110xxxxx 10xxxxxx MY_CS_ILSEQ ERROR 2
        str = Slices.wrappedBuffer((byte) 0xC5, (byte) 0x61);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_ILSEQ);
    }

    @Test
    public void testMB1() {
        // 0x0 - 0x7f | 0xxxxxxx
        str = Slices.wrappedBuffer((byte) 0x78);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, 1);

        // 0x0 - 0x7f | 0xxxxxxx ERROR
        str = Slices.wrappedBuffer((byte) 0xD1);
        result = AbstractCollationHandler
            .codepointOfUTF8(parsedWildCharacter, str, 0, str.length(), true, true);
        Assert.assertEquals(result, MY_CS_TOOSMALL2);
    }
}
