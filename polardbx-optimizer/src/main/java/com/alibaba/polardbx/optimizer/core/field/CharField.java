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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.SQLModeFlags;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTemporalValue;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;

import static com.alibaba.polardbx.common.charset.MySQLUnicodeUtils.LATIN1_TO_UTF8_BYTES;

/**
 * MySQL char(n) data type.
 * Return the full-length characters if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' set.
 */
public class CharField extends AbstractStorageField {
    private static final Logger LOGGER = LoggerFactory.getLogger(CharField.class);
    protected static final int MAX_ERROR_STRING_SIZE = 1 << 4;

    protected byte[] packedBinary;

    private int realLength;

    public CharField(DataType<?> fieldType) {
        super(fieldType);
        Preconditions.checkArgument(fieldType instanceof SliceType);
        packedBinary = new byte[startPos() + fieldType.length()];
    }

    @Override
    protected TypeConversionStatus storeXProtocolInternal(XResult xResult, PolarxResultset.ColumnMetaData meta,
                                                          ByteString byteString, int columnIndex,
                                                          SessionProperties sessionProperties) throws Exception {
        final Pair<Object, byte[]> pair =
            XResultUtil.resultToObject(meta, byteString, true,
                xResult.getConnection().getSession().getDefaultTimezone());
        byte[] utf8Bytes;
        if (pair.getKey() instanceof String) {
            String val = (String) pair.getKey();
            utf8Bytes = val.getBytes();
        } else {
            utf8Bytes = pair.getValue();
        }
        if (utf8Bytes != null) {
            return storeUTF8(utf8Bytes, sessionProperties);
        } else {
            setNull();
            return TypeConversionStatus.TYPE_OK;
        }
    }

    @Override
    protected TypeConversionStatus storeJdbcInternal(ResultSet rs, int columnIndex, SessionProperties sessionProperties)
        throws Exception {
        String val = rs.getString(columnIndex);
        if (val != null) {
            byte[] utf8Bytes = val.getBytes();
            if (utf8Bytes != null) {
                return storeUTF8(utf8Bytes, sessionProperties);
            } else {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
        } else {
            setNull();
            return TypeConversionStatus.TYPE_OK;
        }
    }

    @Override
    protected TypeConversionStatus storeInternal(Object value, DataType<?> resultType,
                                                 SessionProperties sessionProperties) {
        switch (resultType.fieldType()) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            if (value instanceof Number) {
                boolean isDataTypeUnsigned = resultType.isUnsigned();
                long longValue = ((Number) value).longValue();
                return storeLong(longValue, isDataTypeUnsigned, sessionProperties);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
            if (value instanceof OriginalTemporalValue) {
                // store value from chunk executor as mysql datetime.
                OriginalTemporalValue temporalValue = (OriginalTemporalValue) value;
                Object converted = DataTypeUtil.convert(resultType, fieldType, temporalValue);
                String timeStr;
                if (converted instanceof Slice) {
                    timeStr = ((Slice) converted).toStringUtf8();
                } else if (converted instanceof String) {
                    timeStr = (String) converted;
                } else if (converted instanceof byte[]) {
                    timeStr = new String((byte[]) converted);
                } else {
                    // don't accept null value because it's the failed result from implicit cast.
                    return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
                }

                return storeUTF8(timeStr.getBytes(), sessionProperties);
            } else if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            // bytes in UTF-8 character set.
            if (value instanceof Slice) {
                Slice slice = (Slice) value;
                byte[] utf8Bytes = slice.getBytes();
                return storeUTF8(utf8Bytes, sessionProperties);
            }

            // chars in unicode format
            if (value instanceof String) {
                return storeUnicodeChars((String) value, sessionProperties);
            }

            // bytes in any character set
            if (value instanceof org.apache.calcite.avatica.util.ByteString) {
                org.apache.calcite.avatica.util.ByteString byteString =
                    (org.apache.calcite.avatica.util.ByteString) value;
                return storeBytes(byteString.getBytes());
            } else if (value instanceof byte[]) {
                return storeBytes((byte[]) value);
            }

            // for null value.
            if (value == null) {
                setNull();
                return TypeConversionStatus.TYPE_OK;
            }
            break;
        default:
            return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
        }
        return TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_STRING;
    }

    @Override
    public void reset() {
        Arrays.fill(packedBinary, (byte) 0);
        isNull = false;
    }

    @Override
    public void setNull() {
        Arrays.fill(packedBinary, (byte) 0);
        isNull = true;
    }

    @Override
    public void hash(long[] numbers) {
        long nr1 = numbers[0];
        long nr2 = numbers[1];
        if (isNull()) {
            nr1 ^= (nr1 << 1) | 1;
            numbers[0] = nr1;
        } else {
            int length = packetLength();
            CollationHandler collationHandler = getCollationHandler();
            collationHandler.hashcode(packedBinary, length, numbers);
        }
    }

    @Override
    public long xxHashCode() {
        if (isNull()) {
            return NULL_HASH_CODE;
        }
        if (isLatin1CharSet() && !isBinaryCollation()) {
            return getCollationHandler().hashcode(convertLatin1ToUtf8(packedBinary, 0, realLength));
        } else {
            return getCollationHandler().hashcode(Slices.wrappedBuffer(packedBinary, 0, realLength));
        }
    }

    protected boolean isLatin1CharSet() {
        return getCharsetHandler().getName() == CharsetName.LATIN1;
    }

    protected boolean isBinaryCollation() {
        return getCollationHandler().getName() == CollationName.BINARY || getCollationHandler().getName().name()
            .endsWith("_BIN");
    }

    protected Slice convertLatin1ToUtf8(byte[] packedBinary, int startPos, int length) {
        SliceOutput sliceOutput = new DynamicSliceOutput(length);
        for (int i = 0; i < length; ++i) {
            sliceOutput.writeBytes(LATIN1_TO_UTF8_BYTES[((int) packedBinary[i + startPos]) & 0xFF]);
        }
        return sliceOutput.slice();
    }

    @Override
    public void makeSortKey(byte[] result, int len) {
        // get slice to cal sort key.
        Slice original = Slices.wrappedBuffer(packedBinary, 0, packetLength());

        CollationHandler collationHandler = getCollationHandler();
        SortKey sortKey = collationHandler.getSortKey(original, len);

        // copy from sort key bytes
        System.arraycopy(sortKey.keys, 0, result, 0, Math.min(len, sortKey.keys.length));
    }

    @Override
    public MysqlDateTime datetimeValue(int timeParseFlags, SessionProperties sessionProperties) {
        Slice utf8Slice = stringValue();
        byte[] toParse = (byte[]) utf8Slice.getBase();

        MysqlDateTime t = StringTimeParser.parseString(toParse, MySQLTimeTypeUtil.DATETIME_SQL_TYPE, timeParseFlags);
        return t;
    }

    @Override
    public MysqlDateTime timeValue(int timeParseFlags, SessionProperties sessionProperties) {
        Slice utf8Slice = stringValue();
        byte[] toParse = (byte[]) utf8Slice.getBase();

        MysqlDateTime t = StringTimeParser.parseString(toParse, Types.TIME, timeParseFlags);
        return t;
    }

    @Override
    public long longValue(SessionProperties sessionProperties) {
        Slice utf8Slice = stringValue();
        if (utf8Slice.length() == 0) {
            return 0L;
        }

        byte[] toParse = (byte[]) utf8Slice.getBase();

        // todo we need implement a utf8-charset str-to-long method.
        long[] parseRes = StringNumericParser.parseString(toParse);
        long res = parseRes[StringNumericParser.NUMERIC_INDEX];
        return res;
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        int end = fieldType.length();
        int begin = startPos();

        // if we have no sql mode of PAD_CHAR_TO_FULL_LENGTH, skip trailing space.
        if (!SQLModeFlags.check(sessionProperties.getSqlModeFlag(), SQLMode.PAD_CHAR_TO_FULL_LENGTH.getModeFlag())) {
            while (end >= begin + 1 && packedBinary[end - 1] == 0x20) {
                end--;
            }
        }
        Slice original = Slices.wrappedBuffer(packedBinary, 0, end);
        if (!fieldType.isUtf8Encoding()) {
            // convert to utf-8
            CharsetHandler charsetHandler = getCharsetHandler();
            String unicodeStr = charsetHandler.decode(original);
            return CharsetFactory.DEFAULT_CHARSET_HANDLER.encodeWithReplace(unicodeStr);
        } else {
            return Slices.copyOf(original);
        }
    }

    @Override
    public byte[] rawBytes() {
        return packedBinary;
    }

    @Override
    int calPacketLength() {
        return fieldType.length();
    }

    @Override
    public CollationHandler getCollationHandler() {
        return ((SliceType) fieldType).getCollationHandler();
    }

    protected TypeConversionStatus storeUTF8(byte[] fromBytes, SessionProperties sessionProperties) {
        return storeUTF8(fromBytes, fromBytes.length, sessionProperties);
    }

    /**
     * Store utf-8 bytes to destination character set field.
     *
     * @return the last index in packed binary array.
     */
    protected TypeConversionStatus storeUTF8(byte[] fromBytes, int len, SessionProperties sessionProperties) {
        TypeConversionStatus typeConversionStatus;

        int lengthOfCopy = 0;
        boolean hasWellFormedError = false;
        boolean hasConvertError = false;
        boolean isTruncated = false;
        boolean isTruncatedImportant = false;

        CharsetHandler charsetHandler = getCharsetHandler();
        CollationHandler collationHandler = getCollationHandler();

        final int fromLen = len;
        int fieldLen = fieldType.length();
        int charNumbers = (fieldLen) / charsetHandler.maxLenOfMultiBytes();
        boolean isBinary = fieldType.getCharsetName() == CharsetName.BINARY;

        // store to utf8/binary field
        if (fieldType.isUtf8Encoding() || isBinary) {
            try {
                // too small
                if (fieldLen < charsetHandler.minLenOfMultiBytes() || charNumbers == 0) {
                    lengthOfCopy = 0;
                }
                int fromOffset = fromLen % charsetHandler.minLenOfMultiBytes();
                if (isBinary) {
                    // For binary field, just copy bytes.
                    lengthOfCopy = Math.min(Math.min(charNumbers, fieldLen), fromLen);
                    System.arraycopy(fromBytes, 0, packedBinary, startPos(), lengthOfCopy);
                } else {
                    lengthOfCopy = scanChar(fromBytes, charsetHandler, fromLen, charNumbers, fromOffset);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof CharField.WellFormException) {
                    hasWellFormedError = true;
                } else if (throwable instanceof CharacterCodingException) {
                    hasConvertError = true;
                }
                isNull = false;
            }

            typeConversionStatus =
                checkStringError(fromBytes, len, lengthOfCopy, hasWellFormedError, hasConvertError, collationHandler,
                    countSpace());
        } else {
            try {
                // from utf8 bytes to unicode chars
                CharsetHandler fromCharsetHandler = CharsetFactory.DEFAULT_CHARSET_HANDLER;
                String unicodeChars = fromCharsetHandler.decode(fromBytes);
                // cut to char numbers.
                String originalChars = unicodeChars;
                if (originalChars.length() > charNumbers) {
                    unicodeChars = unicodeChars.substring(0, charNumbers);
                    isTruncated = true;
                    // check if truncated part is important or not.
                    for (int charPos = charNumbers; charPos < originalChars.length(); charPos++) {
                        if (originalChars.charAt(charPos) != ' ') {
                            isTruncatedImportant = true;
                            break;
                        }
                    }
                }
                // from unicode character to specific bytes
                Slice nonUTF8Bytes = null;
                try {
                    nonUTF8Bytes = charsetHandler.encode(unicodeChars);
                } catch (CharacterCodingException e) {
                    LOGGER.warn(
                        "un-mappable characters: " + showErrorBytes(fromBytes) + ", for character set: " + fieldType
                            .getCharsetName().name());
                    throw e;
                }

                lengthOfCopy = nonUTF8Bytes.length();
                nonUTF8Bytes.getBytes(0, packedBinary, startPos(), lengthOfCopy);
            } catch (Throwable throwable) {
                if (throwable instanceof CharField.WellFormException) {
                    hasWellFormedError = true;
                } else if (throwable instanceof CharacterCodingException) {
                    hasConvertError = true;
                }
                isNull = false;
            }

            typeConversionStatus =
                checkStringError(hasWellFormedError, hasConvertError, isTruncated, isTruncatedImportant, countSpace());
        }

        postHandle(lengthOfCopy);

        return typeConversionStatus;
    }

    protected void postHandle(int lengthOfCopy) {
        // Append spaces if the string was shorter than the field
        if (lengthOfCopy < packetLength()) {
            realLength = lengthOfCopy;
            Arrays.fill(packedBinary, lengthOfCopy, packedBinary.length, getCollationHandler().padChar());
        } else {
            realLength = packetLength();
        }
    }

    protected boolean countSpace() {
        return false;
    }

    /**
     * Store binary hex string to the field.
     *
     * @param fromNonUTF8Bytes from-bytes with from-charset binary.
     * @return Type Conversion Status
     */
    protected TypeConversionStatus storeBytes(byte[] fromNonUTF8Bytes) {
        TypeConversionStatus typeConversionStatus;

        int lengthOfCopy = 0;
        boolean hasWellFormedError = false;
        boolean hasConvertError = false;
        boolean isTruncated = false;
        boolean isTruncatedImportant = false;

        CharsetHandler charsetHandler = getCharsetHandler();

        int fieldLen = fieldType.length();
        int charNumbers = (fieldLen) / charsetHandler.maxLenOfMultiBytes();
        boolean isBinary = fieldType.getCharsetName() == CharsetName.BINARY;

        try {
            // from utf8 bytes to unicode chars
            String unicodeChars = charsetHandler.decode(fromNonUTF8Bytes);
            // cut to char numbers.
            String originalChars = unicodeChars;
            if (originalChars.length() > charNumbers) {
                unicodeChars = unicodeChars.substring(0, charNumbers);
                isTruncated = true;
                // check if truncated part is important or not.
                if (isBinary) {
                    isTruncatedImportant = true;
                } else {
                    for (int charPos = charNumbers; charPos < originalChars.length(); charPos++) {
                        if (originalChars.charAt(charPos) != ' ') {
                            isTruncatedImportant = true;
                            break;
                        }
                    }
                }
            }
            // well-formed check
            Slice nonUTF8Bytes = null;
            try {
                nonUTF8Bytes = charsetHandler.encode(unicodeChars);
            } catch (CharacterCodingException e) {
                LOGGER.warn(
                    "un-mappable characters: " + showErrorBytes(fromNonUTF8Bytes) + ", for character set: " + fieldType
                        .getCharsetName().name());
                throw e;
            }

            lengthOfCopy = nonUTF8Bytes.length();

            // copy the bytes to ptr with specific length
            nonUTF8Bytes.getBytes(0, packedBinary, startPos(), lengthOfCopy);
        } catch (Throwable throwable) {
            if (throwable instanceof CharField.WellFormException) {
                hasWellFormedError = true;
            } else if (throwable instanceof CharacterCodingException) {
                hasConvertError = true;
            }
            isNull = false;
        }

        typeConversionStatus =
            checkStringError(hasWellFormedError, hasConvertError, isTruncated, isTruncatedImportant, countSpace());

        postHandle(lengthOfCopy);

        return typeConversionStatus;
    }

    protected TypeConversionStatus storeUnicodeChars(String unicodeChars, SessionProperties sessionProperties) {
        TypeConversionStatus typeConversionStatus;

        int lengthOfCopy = 0;
        boolean hasWellFormedError = false;
        boolean hasConvertError = false;
        boolean isTruncated = false;
        boolean isTruncatedImportant = false;

        CharsetHandler charsetHandler = getCharsetHandler();
        CollationHandler collationHandler = getCollationHandler();

        int fieldLen = fieldType.length();
        int charNumbers = (fieldLen) / charsetHandler.maxLenOfMultiBytes();
        boolean isBinary = fieldType.getCharsetName() == CharsetName.BINARY;

        try {
            // cut to char numbers.
            String originalChars = unicodeChars;
            if (originalChars.length() > charNumbers) {
                unicodeChars = unicodeChars.substring(0, charNumbers);
                isTruncated = true;
                // check if truncated part is important or not.
                if (isBinary) {
                    isTruncatedImportant = true;
                } else {
                    for (int charPos = charNumbers; charPos < originalChars.length(); charPos++) {
                        if (originalChars.charAt(charPos) != ' ') {
                            isTruncatedImportant = true;
                            break;
                        }
                    }
                }
            }
            // from unicode character to specific bytes
            Slice nonUTF8Bytes = null;
            try {
                nonUTF8Bytes = charsetHandler.encode(unicodeChars);
            } catch (CharacterCodingException e) {
                LOGGER.warn(
                    "un-mappable characters: " + showErrorString(unicodeChars) + ", for character set: " + fieldType
                        .getCharsetName().name());
                throw e;
            }

            lengthOfCopy = nonUTF8Bytes.length();
            nonUTF8Bytes.getBytes(0, packedBinary, startPos(), lengthOfCopy);
        } catch (Throwable throwable) {
            if (throwable instanceof CharField.WellFormException) {
                hasWellFormedError = true;
            } else if (throwable instanceof CharacterCodingException) {
                hasConvertError = true;
            }
            isNull = false;
        }

        typeConversionStatus =
            checkStringError(hasWellFormedError, hasConvertError, isTruncated, isTruncatedImportant, countSpace());

        postHandle(lengthOfCopy);

        return typeConversionStatus;
    }

    /**
     * Report "not well formed" or "cannot convert" error after storing a character string info a field.
     */
    protected TypeConversionStatus checkStringError(boolean hasWellFormedError, boolean hasConvertError,
                                                    boolean isTruncated, boolean isImportantData, boolean countSpace) {
        if (!hasWellFormedError && !hasConvertError) {
            // check import data truncated
            if (isTruncated) {
                if (isImportantData) {
                    return TypeConversionStatus.TYPE_WARN_TRUNCATED;
                } else if (countSpace) {
                    return TypeConversionStatus.TYPE_NOTE_TRUNCATED;
                }
            }
            return TypeConversionStatus.TYPE_OK;
        } else {
            return TypeConversionStatus.TYPE_WARN_INVALID_STRING;
        }
    }

    /**
     * Report "not well formed" or "cannot convert" error after storing a character string info a field.
     */
    protected TypeConversionStatus checkStringError(byte[] fromBytes, int len, int lengthOfCopy,
                                                    boolean hasWellFormedError, boolean hasConvertError,
                                                    CollationHandler collationHandler, boolean countSpace) {
        if (!hasWellFormedError && !hasConvertError) {
            // check import data truncated
            if (lengthOfCopy < len) {
                // test if important data
                boolean isImportantData =
                    isImportantData(fromBytes, lengthOfCopy, len, collationHandler.getCharsetName());
                if (isImportantData) {
                    return TypeConversionStatus.TYPE_WARN_TRUNCATED;
                } else if (countSpace) {
                    return TypeConversionStatus.TYPE_NOTE_TRUNCATED;
                }
            }
            return TypeConversionStatus.TYPE_OK;
        } else {
            return TypeConversionStatus.TYPE_WARN_INVALID_STRING;
        }
    }

    /**
     * Check if we lost any important data and send a truncation error/warning
     */
    protected boolean isImportantData(byte[] bytes, int from, int end, CharsetName charsetName) {
        if (from >= end) {
            return false;
        }

        boolean isImportantData = false;
        if (charsetName != CharsetName.BINARY) {
            // for non-binary charset, take space as unimportant data.
            for (int i = from; i < end; i++) {
                if (bytes[i] != 0x20) {
                    isImportantData = true;
                    break;
                }
            }
        } else {
            isImportantData = true;
        }
        return isImportantData;
    }

    private int scanChar(byte[] fromBytes, CharsetHandler charsetHandler, int fromLen, int charNumbers,
                         int fromOffset) throws WellFormException {
        int lengthOfCopy;

        // We suppose that the min-bytes of any character set is 1, so we use fromLen/1 as the max character number.
        if (fromLen > packetLength() || fromLen / 1 > charNumbers) {
            // For utf-8 field, calculate the well-formed multi-bytes len
            SliceInput sliceInput = Slices.wrappedBuffer(fromBytes, 0, fromLen).getInput();
            int charLenToCheck = charNumbers;

            // Bugfix: when the character number is less than varchar number in column definition,
            // but the length of byte array is more than varchar number in column definition.
            while (sliceInput.isReadable() && charLenToCheck > 0) {
                if (charsetHandler.nextChar(sliceInput) == CharsetHandler.INVALID_CODE) {
                    // handle well-form error
                    LOGGER.warn("un-mappable characters: " + showErrorBytes(fromBytes) + ", for character set: "
                        + fieldType.getCharsetName().name());
                    throw new WellFormException();
                }
                charLenToCheck--;
            }
            lengthOfCopy = (int) sliceInput.position();
            System.arraycopy(fromBytes, 0, packedBinary, startPos(), lengthOfCopy);
            if (fromOffset != 0) {
                lengthOfCopy += fromOffset;
            }
        } else {
            // if shorter than packed length, just copy
            lengthOfCopy = fromLen;
            System.arraycopy(fromBytes, 0, packedBinary, startPos(), lengthOfCopy);
        }
        return lengthOfCopy;
    }

    protected TypeConversionStatus storeLong(long l, boolean isDataTypeUnsigned, SessionProperties sessionProperties) {
        byte[] result = new byte[64];
        CharsetHandler charsetHandler = getCharsetHandler();
        int resultLen = charsetHandler.parseFromLong(l, isDataTypeUnsigned, result, 64);
        return storeUTF8(result, resultLen, sessionProperties);
    }

    private String showErrorBytes(byte[] fromBytes) {
        String errorStr = CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.LATIN1).decode(fromBytes);
        return showErrorString(errorStr);
    }

    private String showErrorString(String errorStr) {
        if (errorStr.length() > MAX_ERROR_STRING_SIZE) {
            return errorStr.substring(0, MAX_ERROR_STRING_SIZE);
        }
        return errorStr;
    }

    /**
     * The start position of characters in binary storage.
     */
    protected int startPos() {
        return 0;
    }

    /**
     * For unicode parsing error.
     */
    protected class WellFormException extends java.io.IOException {
        public WellFormException() {
        }
    }
}
