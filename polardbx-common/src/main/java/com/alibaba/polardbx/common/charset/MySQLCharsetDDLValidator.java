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

package com.alibaba.polardbx.common.charset;

import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.Optional;

/**
 * Utility to check the validity of charset and collation name in DDL.
 */
public class MySQLCharsetDDLValidator {
    /**
     * Check the validity of collation name string.
     *
     * @param collationNameStr collation name string.
     * @return the validity
     */
    public static boolean checkCollation(String collationNameStr) {
        return Optional.ofNullable(collationNameStr)
            .map(String::toUpperCase)
            .map(CollationName.COLLATION_NAME_MAP::containsKey)
            .orElse(false);
    }

    /**
     * Check the validity of charset name string.
     *
     * @param charsetNameStr charset name string.
     * @return the validity
     */
    public static boolean checkCharset(String charsetNameStr) {
        return Optional.ofNullable(charsetNameStr)
            .map(String::toUpperCase)
            .map(CharsetName.CHARSET_NAME_MAP::containsKey)
            .orElse(false);
    }

    /**
     * Check the validity of charset & collation name string.
     *
     * @return TRUE if the name is valid and the collation match the charset.
     */
    public static boolean checkCharsetCollation(String charsetNameStr, String collationNameStr) {
        boolean isCollationValid = Optional.ofNullable(collationNameStr)
            .map(String::toUpperCase)
            .map(CollationName.COLLATION_NAME_MAP::containsKey)
            .orElse(false);

        boolean isCharsetValid = Optional.ofNullable(charsetNameStr)
            .map(String::toUpperCase)
            .map(CharsetName.CHARSET_NAME_MAP::containsKey)
            .orElse(false);

        if (isCollationValid && isCharsetValid) {
            CharsetName charsetName = CharsetName.of(charsetNameStr);
            CollationName collationName = CollationName.of(collationNameStr);
            return charsetName.match(collationName);
        }

        return false;
    }

    /**
     * Check if collation has been implemented in PolarDB-X
     */
    public static boolean isCollationImplemented(String collationNameStr) {
        return Optional.ofNullable(collationNameStr)
            .map(CollationName.POLAR_DB_X_IMPLEMENTED_COLLATION_NAME_STRINGS::contains)
            .orElse(false);
    }

    /**
     * Check if charset has been implemented in PolarDB-X
     */
    public static boolean isCharsetImplemented(String charsetNameStr) {
        return Optional.ofNullable(charsetNameStr)
            .map(CharsetName.POLAR_DB_X_IMPLEMENTED_CHARSET_NAME_STRINGS::contains)
            .orElse(false);
    }

    public static boolean checkIfMySql80NewCollation(String collationNameStr) {
        CollationName collationName = CollationName.of(collationNameStr, false);
        if (collationName != null) {
            return collationName.isMySQL80NewSupported();
        }
        return false;
    }

    public static boolean checkCharsetSupported(String charsetStr, String collationStr, boolean checkImplementation) {
        try {
            if (!TStringUtil.isEmpty(charsetStr) && !TStringUtil.isEmpty(collationStr)) {
                CharsetName charsetName = CharsetName.of(charsetStr, false);
                CollationName collationName = CollationName.of(collationStr, false);

                if (charsetName == null) {
                    // Unknown character set
                    return false;
                }
                if (collationName == null) {
                    // Unknown collation
                    return false;
                }

                if (!charsetName.match(collationName)) {
                    // COLLATION is not valid for CHARACTER SET
                    return false;
                }

                boolean isCharsetValid =
                    checkImplementation ? isCharsetImplemented(charsetStr) : checkCharset(charsetStr);
                if (!isCharsetValid) {
                    // charset unimplemented
                    return false;
                }

                boolean isCollationValid =
                    checkImplementation ? isCollationImplemented(collationStr) : checkCollation(collationStr);
                if (!isCollationValid) {
                    // collation unimplemented
                    return false;
                }

                return true;
            } else if (TStringUtil.isEmpty(charsetStr) && !TStringUtil.isEmpty(collationStr)) {
                CollationName collationName = CollationName.of(collationStr, false);
                if (collationName == null) {
                    // Unknown collation
                    return false;
                }

                boolean isCollationValid =
                    checkImplementation ? isCollationImplemented(collationStr) : checkCollation(collationStr);
                if (!isCollationValid) {
                    // collation unimplemented
                    return false;
                }

                return true;
            } else if (!TStringUtil.isEmpty(charsetStr) && TStringUtil.isEmpty(collationStr)) {
                CharsetName charsetName = CharsetName.of(charsetStr, false);
                if (charsetName == null) {
                    // Unknown character set
                    return false;
                }

                boolean isCharsetValid =
                    checkImplementation ? isCharsetImplemented(charsetStr) : checkCharset(charsetStr);
                if (!isCharsetValid) {
                    // charset unimplemented
                    return false;
                }

                return true;
            } else {
                return true;
            }

        } catch (Throwable ex) {
            return false;
        }
    }

    /**
     * Check if supported for charsetStr and collationStr.
     * The charsetStr and collationStr are nullable.
     */
    public static boolean checkCharsetSupported(String charsetStr, String collationStr) {
        return checkCharsetSupported(charsetStr, collationStr, false);
    }
}
