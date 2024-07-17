package com.alibaba.polardbx.optimizer.core.rule;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.Charset;

@RunWith(MockitoJUnitRunner.class)
public class RuleUtilsTest {
    class TypeInfo {
        SqlTypeName typeName;

        Charset charSet;

        SqlCollation collation;

        public TypeInfo(SqlTypeName typeName, Charset charSet, SqlCollation collation) {
            this.typeName = typeName;
            this.charSet = charSet;
            this.collation = collation;
        }
    }

    @Test
    public void testIntInt() {
        TypeInfo typeInfo1 = new TypeInfo(SqlTypeName.INTEGER, null, null);
        TypeInfo typeInfo2 = new TypeInfo(SqlTypeName.INTEGER, null, null);
        test(typeInfo1, typeInfo2, true);
    }

    @Test
    public void testCharsetNull() {
        TypeInfo typeInfo1 = new TypeInfo(SqlTypeName.CHAR, null, new SqlCollation("utf8mb4$utf8mb4_bin",
            SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testCollationNull() {
        TypeInfo typeInfo1 = new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), null);
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testUtf8Equal() {
        TypeInfo typeInfo1 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, true);
    }

    @Test
    public void testUtf8Equal2() {
        TypeInfo typeInfo1 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.VARCHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, true);
    }

    @Test
    public void testMixedCollation() {
        TypeInfo typeInfo1 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_general_ci",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testMixedCharset() {
        TypeInfo typeInfo1 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("latin1"), new SqlCollation("latin1$latin1_bin",
                SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_general_ci",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testVarchar() {
        TypeInfo typeInfo1 =
            new TypeInfo(SqlTypeName.VARCHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_bin",
                SqlCollation.Coercibility.EXPLICIT));
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.VARCHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_general_ci",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testIntFloat() {
        TypeInfo typeInfo1 = new TypeInfo(SqlTypeName.INTEGER, null, null);
        TypeInfo typeInfo2 = new TypeInfo(SqlTypeName.FLOAT, null, null);
        test(typeInfo1, typeInfo2, false);
    }

    @Test
    public void testIntChar() {
        TypeInfo typeInfo1 = new TypeInfo(SqlTypeName.INTEGER, null, null);
        TypeInfo typeInfo2 =
            new TypeInfo(SqlTypeName.CHAR, Charset.forName("utf8mb4"), new SqlCollation("utf8mb4$utf8mb4_general_ci",
                SqlCollation.Coercibility.EXPLICIT));
        test(typeInfo1, typeInfo2, false);
    }

    public void test(TypeInfo typeInfo1, TypeInfo typeInfo2, boolean same) {
        RelDataType type1 = Mockito.mock(RelDataType.class);
        Mockito.when(type1.getSqlTypeName()).thenReturn(typeInfo1.typeName);
        Mockito.when(type1.getCharset()).thenReturn(typeInfo1.charSet);
        Mockito.when(type1.getCollation()).thenReturn(typeInfo1.collation);

        RelDataType type2 = Mockito.mock(RelDataType.class);
        Mockito.when(type2.getSqlTypeName()).thenReturn(typeInfo2.typeName);
        Mockito.when(type2.getCharset()).thenReturn(typeInfo2.charSet);
        Mockito.when(type2.getCollation()).thenReturn(typeInfo2.collation);

        Assert.assertTrue(RuleUtils.sameType(type1, type2) == same);
    }
}
