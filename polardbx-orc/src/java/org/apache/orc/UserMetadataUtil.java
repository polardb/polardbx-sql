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
