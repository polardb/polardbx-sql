package org.apache.orc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class UserMetadataUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    public static final String ENABLE_DECIMAL_64 = "ENABLE_DECIMAL_64";

    public static boolean parseBooleanValue(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String decodeValue(ByteBuffer value) {
        try {
            final CharsetDecoder UTF8_DECODER = StandardCharsets.UTF_8.newDecoder();
            return UTF8_DECODER.decode(value).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean extractBooleanValue(List<OrcProto.UserMetadataItem> metadataItemList, String key,
                                              boolean defaultValue) {
        if (metadataItemList == null || metadataItemList.isEmpty()) {
            return defaultValue;
        }
        try {
            for (OrcProto.UserMetadataItem item: metadataItemList) {
                if (item.hasName() && item.getName().equals(key)) {
                    ByteBuffer byteBuffer = item.getValue().asReadOnlyByteBuffer();
                    String strValue = decodeValue(byteBuffer);
                    return parseBooleanValue(strValue, defaultValue);
                }
            }
            return defaultValue;
        } catch (Exception e) {
            LOGGER.error("Failed to extract key from metadata: " + key, e);
            return defaultValue;
        }
    }
}
