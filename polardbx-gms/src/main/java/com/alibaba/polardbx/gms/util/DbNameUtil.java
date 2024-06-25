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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chenghui.lch
 */
public class DbNameUtil {

    private final static int MAX_DB_NAME_LENGTH = 64;

    public static boolean validateDbName(String dbName, boolean isKeyWords) {

        if (dbName.length() > MAX_DB_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the length of dbName[%s] is too long", dbName));
            //return false;
        }

        for (int i = 0; i < dbName.length(); i++) {
            if (!isWord(dbName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Failed to create database because the dbName[%s] contains some invalid characters",
                        dbName));
            }
        }

        if (isKeyWords) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the string of dbName[%s] is a keyword", dbName));
        }

        return true;
    }

    /**
     * check a char if is "a-z、A-Z、_、-、0-9"
     *
     * @return true if check ok
     */
    public static boolean isWord(char c) {
        String regEx = "[\\w-]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher("" + c);
        return m.matches();
    }

}
