package com.alibaba.polardbx.druid.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;

public class SQLUtilsTest {

    @Test
    public void testSplitNames() {
        List<String> expected = Lists.newArrayList("a", "b", "c");
        List<String> names = SQLUtils.splitNamesByComma("a,b,c");
        Assert.assertEquals(expected, names);

        names = SQLUtils.splitNamesByComma("`a`,`b`,`c`");
        Assert.assertEquals(expected, names);

        names = SQLUtils.splitNamesByComma("`a`,b,`c`");
        Assert.assertEquals(expected, names);

        names = SQLUtils.splitNamesByComma(" `a`,b,`c` ");
        Assert.assertEquals(expected, names);

        names = SQLUtils.splitNamesByComma("`a`,b,` `,`c`");
        Assert.assertNotEquals(expected, names);
    }

    @Test
    public void testSplitNamesEscape() {
        List<String> expected = Lists.newArrayList("a", "b1,b2", "c");
        List<String> names = SQLUtils.splitNamesByComma("`a`,`b1,b2`,`c`");
        Assert.assertEquals(expected, names);

        expected = Lists.newArrayList("a", "b1,`b2", "c");
        names = SQLUtils.splitNamesByComma("`a`,`b1,``b2`,`c`");
        Assert.assertEquals(expected, names);
    }

    @Test
    public void testMockSetEngineName() {
        SQLIndexDefinition mockIndexDefinition = mock(SQLIndexDefinition.class);
        MySqlUnique sqlUnique = new MySqlUnique();
        SQLName mockEngineName = mock(SQLName.class);
        sqlUnique.setEngineName(mockEngineName);
    }
}
