package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JavaUdfCreateTest extends BaseTestCase {

    private static Connection connection;

    private static String FUNC_NAME = "test_udf_create";

    @Before
    public void init() throws SQLException {
        connection = getPolardbxConnection();
    }

    @Test
    public void testCreateJavaUdfSuccess() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop java function if exists %s", FUNC_NAME));
        String sql = String.format("create java function %s RETURN_TYPE BIGINT "
            + "CODE "
            + "public class %s extends UserDefinedJavaFunction "
            + "{             "
            + "     public Object compute(Object[] args) {       "
            + "         return 55;     "
            + "     }"
            + "} end_code", FUNC_NAME, StringUtils.funcNameToClassName(FUNC_NAME));
        JdbcUtil.executeSuccess(connection, sql);
    }

    @Test
    public void testCreateMurmurHashSuccess() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop java function if exists %s", "murmurhash128"));
        String sql = "CREATE JAVA FUNCTION murmurhash128\n"
            + "no state\n"
            + "RETURN_TYPE bigint\n"
            + "INPUT_TYPES bigint\n"
            + "CODE \n"
            + "\n"
            + "import java.nio.ByteBuffer;\n"
            + "import java.nio.ByteOrder;\n"
            + "\n"
            + "public class Murmurhash128 extends UserDefinedJavaFunction {\n"
            + "\n"
            + "    private static final int CHUNK_SIZE = 16;\n"
            + "    private static final long C1 = 0x87c37b91114253d5L;\n"
            + "    private static final long C2 = 0x4cf5ad432745937fL;\n"
            + "\n"
            + "    private static final int UNSIGNED_MASK = 0xFF;\n"
            + "\n"
            + "    private long h1;\n"
            + "    private long h2;\n"
            + "    private int length;\n"
            + "\n"
            + "    private ByteBuffer buffer;\n"
            + "\n"
            + "    private int bufferSize;\n"
            + "\n"
            + "    private int chunkSize;\n"
            + "\n"
            + "    // 加一点点中文\n"
            + "    public Object compute(Object[] args) {\n"
            + "        init(0);\n"
            + "        Long data = (Long) args[0];\n"
            + "        putLong(data);\n"
            + "        return hash();\n"
            + "    }\n"
            + "\n"
            + "    public void init(int seed) {\n"
            + "        this.chunkSize = CHUNK_SIZE;\n"
            + "        this.bufferSize = CHUNK_SIZE;\n"
            + "        this.buffer = ByteBuffer.allocate(bufferSize + 7).order(ByteOrder.LITTLE_ENDIAN);\n"
            + "        this.h1 = seed;\n"
            + "        this.h2 = seed;\n"
            + "        this.length = 0;\n"
            + "    }\n"
            + "\n"
            + "    public final void putLong(long l) {\n"
            + "        buffer.putLong(l);\n"
            + "        munchIfFull();\n"
            + "    }\n"
            + "\n"
            + "    private void munchIfFull() {\n"
            + "        if (buffer.remaining() < 8) {\n"
            + "            munch();\n"
            + "        }\n"
            + "    }\n"
            + "\n"
            + "    private void munch() {\n"
            + "        buffer.flip();\n"
            + "        while (buffer.remaining() >= chunkSize) {\n"
            + "            process(buffer);\n"
            + "        }\n"
            + "        buffer.compact();\n"
            + "    }\n"
            + "\n"
            + "    protected void process(ByteBuffer bb) {\n"
            + "        long k1 = bb.getLong();\n"
            + "        long k2 = bb.getLong();\n"
            + "        bmix64(k1, k2);\n"
            + "        length += CHUNK_SIZE;\n"
            + "    }\n"
            + "\n"
            + "    public final Long hash() {\n"
            + "        munch();\n"
            + "        buffer.flip();\n"
            + "        if (buffer.remaining() > 0) {\n"
            + "            processRemaining(buffer);\n"
            + "            buffer.position(buffer.limit());\n"
            + "        }\n"
            + "        return makeHash();\n"
            + "    }\n"
            + "\n"
            + "    private void bmix64(long k1, long k2) {\n"
            + "        h1 ^= mixK1(k1);\n"
            + "\n"
            + "        h1 = Long.rotateLeft(h1, 27);\n"
            + "        h1 += h2;\n"
            + "        h1 = h1 * 5 + 0x52dce729;\n"
            + "\n"
            + "        h2 ^= mixK2(k2);\n"
            + "\n"
            + "        h2 = Long.rotateLeft(h2, 31);\n"
            + "        h2 += h1;\n"
            + "        h2 = h2 * 5 + 0x38495ab5;\n"
            + "    }\n"
            + "\n"
            + "    protected void processRemaining(ByteBuffer bb) {\n"
            + "        long k1 = 0;\n"
            + "        long k2 = 0;\n"
            + "        length += bb.remaining();\n"
            + "        switch (bb.remaining()) {\n"
            + "        case 15:\n"
            + "            k2 ^= (long) toInt(bb.get(14)) << 48; // fall through\n"
            + "        case 14:\n"
            + "            k2 ^= (long) toInt(bb.get(13)) << 40; // fall through\n"
            + "        case 13:\n"
            + "            k2 ^= (long) toInt(bb.get(12)) << 32; // fall through\n"
            + "        case 12:\n"
            + "            k2 ^= (long) toInt(bb.get(11)) << 24; // fall through\n"
            + "        case 11:\n"
            + "            k2 ^= (long) toInt(bb.get(10)) << 16; // fall through\n"
            + "        case 10:\n"
            + "            k2 ^= (long) toInt(bb.get(9)) << 8; // fall through\n"
            + "        case 9:\n"
            + "            k2 ^= (long) toInt(bb.get(8)); // fall through\n"
            + "        case 8:\n"
            + "            k1 ^= bb.getLong();\n"
            + "            break;\n"
            + "        case 7:\n"
            + "            k1 ^= (long) toInt(bb.get(6)) << 48; // fall through\n"
            + "        case 6:\n"
            + "            k1 ^= (long) toInt(bb.get(5)) << 40; // fall through\n"
            + "        case 5:\n"
            + "            k1 ^= (long) toInt(bb.get(4)) << 32; // fall through\n"
            + "        case 4:\n"
            + "            k1 ^= (long) toInt(bb.get(3)) << 24; // fall through\n"
            + "        case 3:\n"
            + "            k1 ^= (long) toInt(bb.get(2)) << 16; // fall through\n"
            + "        case 2:\n"
            + "            k1 ^= (long) toInt(bb.get(1)) << 8; // fall through\n"
            + "        case 1:\n"
            + "            k1 ^= (long) toInt(bb.get(0));\n"
            + "            break;\n"
            + "        default:\n"
            + "            throw new AssertionError(\"Should never get here.\");\n"
            + "        }\n"
            + "        h1 ^= mixK1(k1);\n"
            + "        h2 ^= mixK2(k2);\n"
            + "    }\n"
            + "\n"
            + "    public static int toInt(byte value) {\n"
            + "        return value & UNSIGNED_MASK;\n"
            + "    }\n"
            + "\n"
            + "    public long makeHash() {\n"
            + "        h1 ^= length;\n"
            + "        h2 ^= length;\n"
            + "\n"
            + "        h1 += h2;\n"
            + "        h2 += h1;\n"
            + "\n"
            + "        h1 = fmix64(h1);\n"
            + "        h2 = fmix64(h2);\n"
            + "\n"
            + "        h1 += h2;\n"
            + "        h2 += h1;\n"
            + "\n"
            + "        byte[] array = ByteBuffer.wrap(new byte[CHUNK_SIZE])\n"
            + "            .order(ByteOrder.LITTLE_ENDIAN)\n"
            + "            .putLong(h1)\n"
            + "            .putLong(h2)\n"
            + "            .array();\n"
            + "        return asLong(array);\n"
            + "    }\n"
            + "\n"
            + "    public long asLong(byte[] bytes) {\n"
            + "        checkState(\n"
            + "            bytes.length >= 8,\n"
            + "            \"HashCode#asLong() requires >= 8 bytes (it only has %s bytes).\",\n"
            + "            bytes.length);\n"
            + "        return padToLong(bytes);\n"
            + "    }\n"
            + "\n"
            + "    public long padToLong(byte[] bytes) {\n"
            + "        long retVal = (bytes[0] & 0xFF);\n"
            + "        for (int i = 1; i < Math.min(bytes.length, 8); i++) {\n"
            + "            retVal |= (bytes[i] & 0xFFL) << (i * 8);\n"
            + "        }\n"
            + "        return retVal;\n"
            + "    }\n"
            + "\n"
            + "    private static long fmix64(long k) {\n"
            + "        k ^= k >>> 33;\n"
            + "        k *= 0xff51afd7ed558ccdL;\n"
            + "        k ^= k >>> 33;\n"
            + "        k *= 0xc4ceb9fe1a85ec53L;\n"
            + "        k ^= k >>> 33;\n"
            + "        return k;\n"
            + "    }\n"
            + "\n"
            + "    private static long mixK1(long k1) {\n"
            + "        k1 *= C1;\n"
            + "        k1 = Long.rotateLeft(k1, 31);\n"
            + "        k1 *= C2;\n"
            + "        return k1;\n"
            + "    }\n"
            + "\n"
            + "    private static long mixK2(long k2) {\n"
            + "        k2 *= C2;\n"
            + "        k2 = Long.rotateLeft(k2, 33);\n"
            + "        k2 *= C1;\n"
            + "        return k2;\n"
            + "    }\n"
            + "\n"
            + "    public static void checkState(boolean b, String errorMessageTemplate, int p1) {\n"
            + "        if (!b) {\n"
            + "            throw new IllegalStateException(String.format(errorMessageTemplate, p1));\n"
            + "        }\n"
            + "    }\n"
            + "\n"
            + "} END_CODE";
        JdbcUtil.executeSuccess(connection, sql);
        long hash1 = MurmurHashUtils.murmurHashWithZeroSeed(123);
        System.out.println(hash1);

        ResultSet rs = JdbcUtil.executeQuery("select murmurhash128(123)", connection);
        if (rs.next()) {
            long hash2 = rs.getLong(1);
            System.out.println(hash2);
            Assert.assertTrue(hash1 == hash2,
                String.format("murmurhash result not matched, internal is %s, while java udf is %s", hash1, hash2));
        } else {
            Assert.fail("get empty result from java udf");
        }
    }

    @Test
    public void testCreateJavaUdfFailed() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop java function if exists %s", FUNC_NAME));
        String sql = String.format("create java function %s RETURN_TYPE BIGINT "
            + "CODE "
            + "public class %s extends UserDefinedJavaFunction "
            + "{             "
            + "     public Object compute(Object[] args) {       "
            + "         String var = System.getProperty(\"var\");"
            + "         return 55;     "
            + "     }"
            + "} end_code", FUNC_NAME, StringUtils.funcNameToClassName(FUNC_NAME));
        JdbcUtil.executeFaied(connection, sql, "system cannot be resolved");
    }

    @Test
    public void testCreateJavaUdfFailed2() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop java function if exists %s", FUNC_NAME));
        String sql = String.format("create java function %s RETURN_TYPE BIGINT "
            + "CODE "
            + "public class %s extends UserDefinedJavaFunction "
            + "{             "
            + "     public Object compute(Object[] args) {       "
            + "         Long id = Thread.currentThread().getId();"
            + "         return 55;     "
            + "     }"
            + "} end_code", FUNC_NAME, StringUtils.funcNameToClassName(FUNC_NAME));
        JdbcUtil.executeFaied(connection, sql, "Thread cannot be resolved");
    }

    @After
    public void clear() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop java function if exists %s", FUNC_NAME));
    }
}

