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

package com.alibaba.polardbx.common.encdb.utils;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.alibaba.polardbx.common.encdb.enums.Constants.ENCDB_KEY_SIZE;
import static com.alibaba.polardbx.common.encdb.enums.OrdinalEnum.searchEnum;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    public static final int MIN_ALLOWED_VER_DIFF = 2;

    /* Julian-date equivalents of Day 0 in Unix and Postgres reckoning */
    public static long POSTGRES_EPOCH_JDATE = 2451545; /* == date2j(2000, 1, 1) */
    public static long UNIX_EPOCH_JDATE = 2440588;        /* == date2j(1970, 1, 1) */
    public static long USECS_PER_DAY = 86400000000L;
    public static long SECS_PER_DAY = 86400L;

    public static String bytesTobase64(byte[] param) {
        return Base64.getEncoder().encodeToString(param);
    }

    public static byte[] base64ToBytes(String param) {
        return Base64.getDecoder().decode(param);
    }

    public static String bytesToPgHexString(byte[] data) {
        return "\\x" + Hex.toHexString(data);
    }

    public static byte[] pgHexStringToBytes(String s) {
        return pgHexStringToBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] pgHexStringToBytes(byte[] s) {
        if (s == null) {
            return null;
        }

        // Starting with PG 9.0, a new hex format is supported
        // that starts with "\x". Figure out which format we're
        // dealing with here.
        //
        if (s.length < 2 || s[0] != '\\' || s[1] != 'x') {
            return null;
        }
        return toBytesHexEscaped(s);
    }

    private static byte[] toBytesHexEscaped(byte[] s) {
        byte[] output = new byte[(s.length - 2) / 2];
        for (int i = 0; i < output.length; i++) {
            byte b1 = gethex(s[2 + i * 2]);
            byte b2 = gethex(s[2 + i * 2 + 1]);
            // squid:S3034
            // Raw byte values should not be used in bitwise operations in combination with shifts
            output[i] = (byte) ((b1 << 4) | (b2 & 0xff));
        }
        return output;
    }

    private static byte gethex(byte b) {
        // 0-9 == 48-57
        if (b <= 57) {
            return (byte) (b - 48);
        }

        // a-f == 97-102
        if (b >= 97) {
            return (byte) (b - 97 + 10);
        }

        // A-F == 65-70
        return (byte) (b - 65 + 10);
    }

    public static String getEncdbDir() {
        return "/tmp/encdb-" + System.getProperty("user.name");
    }

    public static String getLocalConfigPath(String name) {
        return getEncdbDir() + "/" + name;
    }

    public static String readResourceStream(String resourcePath) throws IOException {
        InputStream in = Utils.class.getResourceAsStream(resourcePath);
        if (in == null) {
            throw new FileNotFoundException("Resoure '" + resourcePath + "' not found in package.");
        }
        StringBuilder sb = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    public static short getUserID(String username) {
        long hashID = Utils.hashToID(username);

        //hashID is of 8 bytes, hex string of 16 characters
        //!!! hex hashID into 18 characters to make sure that BigInterger is always positive. !!!
        BigInteger biHashID = new BigInteger(Hex.decode(String.format("%018X", hashID)));
        BigInteger m = new BigInteger(String.valueOf(Short.MAX_VALUE));

        return biHashID.mod(m).shortValue();
    }

    public static long hashToID(String valueString) {
        MD5Digest md5 = new MD5Digest();
        byte[] plainBytes = valueString.getBytes();
        byte[] resBuf = new byte[md5.getDigestSize()];
        md5.update(plainBytes, 0, plainBytes.length);
        md5.doFinal(resBuf, 0);

        ByteBuffer buf = ByteBuffer.wrap(resBuf);
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long hashVal = 0;
        while (buf.remaining() > (Long.SIZE >> 8)) {
            hashVal ^= buf.getLong();
        }

        return hashVal;
    }

    public static byte[] readBinaryFromFile(String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    public static void writeBinaryToFile(String filename, byte[] data) throws IOException {
        try (OutputStream outputStream = new FileOutputStream(filename)) {
            outputStream.write(data);
        }
    }

    public static byte[] getRootKeyBytes(String rootKeyStr) {
        if (rootKeyStr == null || rootKeyStr.isEmpty()) {
            return null;
        }

        if (rootKeyStr.toLowerCase().startsWith("0x")) {
            rootKeyStr = rootKeyStr.substring(2);
        }

        if (rootKeyStr.length() != ENCDB_KEY_SIZE * 2) {
            logger.error("expect root key lenght is 16 bytes(32-chars) in hex string format.");
            return null;
        }
        return Hex.decode(rootKeyStr);
    }

    public static void showDemoInfo(String tag, String value) {
        if (!tag.isEmpty()) {
            logger.info("===================== " + tag + " =====================");
        }
        logger.info(value);
    }

    public static List<Byte> swapBytesByPivot(byte[] tmpResult, int index) {
        assert index < tmpResult.length;
        ArrayList<Byte> result = new ArrayList<>();
        for (int i = 0; i < tmpResult.length; ++i) {
            result.add(tmpResult[(index++) % tmpResult.length]);
        }
        return result;
    }

    public static List<Byte> swapBytesByPivot(List<Byte> tmpResult, int index) {
        assert index < tmpResult.size();
        ArrayList<Byte> result = new ArrayList<>();
        for (int i = 0; i < tmpResult.size(); ++i) {
            result.add(tmpResult.get((index++) % tmpResult.size()));
        }
        return result;
    }

    public static byte[] generateIv(int ivLen) {
        byte[] iv = new byte[ivLen];
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    //old format
    //public static String[] splitKeyname(String kName) {
    //    return kName.split("\\|");
    //}

    //new format 需要处理'\\' '|'
    public static String[] splitKeyname(String keyname) {
        if (keyname.length() < 3 || keyname.charAt(0) != '|' || keyname.charAt(keyname.length() - 1) != '|') {
            throw new EncdbException("split keyName error: the first byte and the last byte are not '|'.");
        }

        String[] splitKname = new String[5];
        int splitKnameIdx = 0;
        int beginIndx = 1;
        int escapeCnt = 0;
        for (int i = 1; i < keyname.length(); i++) {
            if (keyname.charAt(i) == '|' && (i == keyname.length() - 1 || escapeCnt % 2 == 0)) {
                if (splitKnameIdx >= 5) {
                    throw new EncdbException("split keyname error:" + keyname + " format error.");
                }
                splitKname[splitKnameIdx] = deformatKeynameComponent(keyname.substring(beginIndx, i));
                escapeCnt = 0;
                beginIndx = i + 1;
                splitKnameIdx++;
            }
            if (keyname.charAt(i) == '\\') {
                escapeCnt += 1;
            } else {
                escapeCnt = 0;
            }
        }
        return splitKname;
    }

    private static String deformatKeynameComponent(String subInput) {
        /* sub '\' before any '|' or '\' character */
        String ret = "";
        if (subInput.length() == 0) {
            throw new EncdbException("deformat keyName error: the subInput length is 0.");
        }
        boolean escape = false;
        for (int i = 0; i < subInput.length(); i++) {
            if (escape) {
                if (subInput.charAt(i) == '|' || subInput.charAt(i) == '\\') {
                    ret += subInput.charAt(i);
                    escape = false;
                } else {
                    throw new EncdbException(
                        "deformat keyName error: the subInput format is error. subInput is:" + subInput);
                }
            } else {
                if (subInput.charAt(i) == '\\') {
                    escape = true;
                } else if (subInput.charAt(i) == '|') {
                    throw new EncdbException(
                        "deformat keyName error: the subInput format is error. subInput is:" + subInput);
                } else {
                    ret += subInput.charAt(i);
                }
            }
        }
        if (escape) {
            throw new EncdbException("deformat keyName error: the subInput format is error. subInput is:" + subInput);
        }
        return ret;
    }

    //old format
    //public static String buildKeyname(String username, String dbname, String tblName, String colName) {
    //    return username + "|" + dbname + "|" + tblName + "|" + colName;
    //}

    //new format 先用 keyStore 方式从 encdb 获取，后面改为 sdk 侧自己生成。因为 sdk 现有架构，buildKeyname 的请求比较多
    public static String buildKeyname(Connection dbConnection, String username, String dbname, String schemaName,
                                      String tblName, String colName) {
        try {
            if (schemaName == null) {
                if (dbConnection == null) {
                    schemaName = "default";
                } else {
                    schemaName = dbConnection.getSchema();
                }
            }
        } catch (Exception throwables) {
            throw new EncdbException("get schema for table:" + tblName + " failed", throwables);
        }
        //String keyname = keyStore.keynameGenerate(username, dbname, schemaName, tblName, colName);
        String keyname = internalBuildKeyname(username, dbname, schemaName, tblName, colName);
        return keyname;
    }

    private static String internalBuildKeyname(String username, String dbname, String schemaName, String tblName,
                                               String colName) {
        ArrayList<String> nameMember = new ArrayList<>();
        nameMember.add(username);
        nameMember.add(dbname);
        nameMember.add(schemaName);
        nameMember.add(tblName);
        nameMember.add(colName);
        int lastNonIdx = -1;
        for (int i = nameMember.size() - 1; i >= 0; i--) {
            if (lastNonIdx >= 0 && (nameMember.get(i) == null || nameMember.get(i).isEmpty())) {
                throw new EncdbException("internal build keyName fail: empty inputs must be consecutive at the end. "
                    + " username:" + username
                    + " dbname:" + dbname
                    + " schemaName:" + schemaName
                    + " tblName:" + tblName
                    + " colName:" + colName);
            }
            if (lastNonIdx < 0 && !(nameMember.get(i) == null) && !(nameMember.get(i).isEmpty())) {
                lastNonIdx = i;
            }
        }
        if (lastNonIdx < 0) {
            throw new EncdbException("internal build keyName error: all inputs are null.");
        }
        //TODO: maybe only username default keyname will be used in future.
        if (dbname == null || dbname.isEmpty()) {
            throw new EncdbException("internal build keyName error: database name cannot be null.");
        }

        String keyname = "|";
        for (int i = 0; i <= lastNonIdx; i++) {
            if (nameMember.get(i) == null || nameMember.get(i).isEmpty()) {
                throw new EncdbException("internal build keyName error: cannot be null for subinputIndex:" + i + ".");
            }
            keyname += formatKeynameComponent(nameMember.get(i)) + "|";
        }
        return keyname;
    }

    private static String formatKeynameComponent(String subInput) {
        /* add '\' before any '|' or '\' character */
        String ret = "";
        for (int i = 0; i < subInput.length(); i++) {
            if (subInput.charAt(i) == '|' || subInput.charAt(i) == '\\') {
                ret += '\\' + subInput.charAt(i);
            } else {
                ret += subInput.charAt(i);
            }
        }
        return ret;
    }

    public static long convertTimeStampNoTimeZoneToMicroSecond(Timestamp val) {
        return val.toInstant().toEpochMilli() * 1000;
    }

    public static Timestamp convertMicroSecondToTimeStampNoTimeZone(long val) {
        return new Timestamp(val / 1000);
    }

    public static int getPatchVer(String currentSdkVer) {
        return Integer.parseInt(currentSdkVer.substring(currentSdkVer.lastIndexOf(".") + 1));
    }

    public static String getMajorMinorVer(String leastClientVer) {
        return leastClientVer.substring(0, leastClientVer.lastIndexOf("."));
    }

    public static byte[] uuidStringToBytes(String uuid_str) {
        String hex_str = uuid_str.replaceAll("-", "");
        if (hex_str.startsWith("{") && hex_str.endsWith("}")) {
            hex_str = hex_str.replaceAll("\\{", "");
            hex_str = hex_str.replaceAll("\\}", "");
        }

        if (hex_str.length() % 2 != 0) {
            throw new EncdbException("invalid uuid composite: \"" + uuid_str + "\"");
        }

        return Hex.decode(hex_str);
    }

    public static String bytesToUuidString(byte[] uuid_bytes) {
        StringBuilder sb = new StringBuilder(Hex.toHexString(uuid_bytes));
        /*
         * We print uuid values as a string of 8, 4, 4, 4, and then 12
         * hexadecimal characters, with each group is separated by a hyphen
         * ("-"). Therefore, add the hyphens at the appropriate places here.
         */
        int[] hyphen_pos = {8, 4, 4, 4, 12};
        char hyphen_char = '-';
        int position = 0;
        for (int i : hyphen_pos) {
            position += i;
            sb.insert(position, hyphen_char);
            position++;
        }
        sb.deleteCharAt(position - 1);

        return sb.toString();
    }

    public static String genRandomUuidString() {
        return UUID.randomUUID().toString();
    }

    public static String toGeneralizedTimeString(Timestamp ts) {
        return new SimpleDateFormat("yyyyMMddHHmmssZ").format(ts);
    }

    public static String currentGeneralizedTimeString() {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        return new SimpleDateFormat("yyyyMMddHHmmssZ").format(ts);
    }
}
