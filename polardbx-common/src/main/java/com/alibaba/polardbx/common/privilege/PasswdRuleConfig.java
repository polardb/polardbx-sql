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

package com.alibaba.polardbx.common.privilege;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.commons.lang.BooleanUtils;

import java.util.LinkedHashSet;
import java.util.Set;

public class PasswdRuleConfig {

    private int minLength = 6;

    private int maxLength = 20;

    private int upperLetter = 0;

    private int lowerLetter = 0;

    private int letter = 0;

    private int digit = 0;

    private boolean specialChar = false;

    private Set<Character> specialCharSet = new LinkedHashSet<>();

    {
        specialCharSet.add('@');
        specialCharSet.add('#');
        specialCharSet.add('$');
        specialCharSet.add('%');
        specialCharSet.add('^');
        specialCharSet.add('&');
        specialCharSet.add('+');
        specialCharSet.add('=');
    }

    public PasswdRuleConfig(int minLength, int maxLength, int upperLetter, int lowerLetter, int letter, int digit,
                            int specialChar) {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.upperLetter = upperLetter;
        this.lowerLetter = lowerLetter;
        this.letter = letter;
        this.digit = digit;
        this.specialChar = BooleanUtils.toBoolean(specialChar);
    }

    public static PasswdRuleConfig parse(JSONObject config) {
        Short min = config.getShort("min");
        Short max = config.getShort("max");
        Short letter = config.getShort("letter");
        Short digit = config.getShort("digit");
        Short upperCase = config.getShort("upperCase");
        Short lowerCase = config.getShort("lowerCase");
        Short specialChar = config.getShort("special");

        final int minLength = (min != null) ? min.intValue() : 6;
        final int maxLength = (max != null) ? max.intValue() : 20;
        final int minLetter = (letter != null) ? letter.intValue() : 0;
        final int minDigit = (digit != null) ? digit.intValue() : 0;
        final int upperLetter = (upperCase != null) ? upperCase.intValue() : 0;
        final int lowerLetter = (lowerCase != null) ? lowerCase.intValue() : 0;

        if (minLength < 0) {
            throw new IllegalArgumentException("Invalid password rule config: min = " + minLength);
        }
        if (maxLength < minLength) {
            throw new IllegalArgumentException(
                "Invalid password rule config: min = " + minLength + ", max = " + maxLength);
        }
        if (minLetter < 0) {
            throw new IllegalArgumentException("Invalid password rule config: letter = " + minLetter);
        }
        if (minDigit < 0) {
            throw new IllegalArgumentException("Invalid password rule config: digit = " + minDigit);
        }
        if (upperLetter < 0) {
            throw new IllegalArgumentException("Invalid password rule config: upperCase = " + upperLetter);
        }
        if (lowerLetter < 0) {
            throw new IllegalArgumentException("Invalid password rule config: lowerCase = " + lowerLetter);
        }
        return new PasswdRuleConfig(minLength, maxLength, upperLetter, lowerLetter, minLetter, minDigit, specialChar);
    }

    public boolean verifyPassword(String password) {
        if (password == null || password.length() < this.minLength || password.length() > this.maxLength) {

            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects " + this.minLength + "-"
                    + this.maxLength + " characters.",
                null);
        }

        int upperLetter = 0;
        int lowerLetter = 0;
        int letter = 0;
        int digit = 0;
        int special = 0;
        for (char ch : password.toCharArray()) {
            if (Character.isLetter(ch)) {
                letter++;
            }
            if (Character.isUpperCase(ch)) {
                upperLetter++;
            }
            if (Character.isLowerCase(ch)) {
                lowerLetter++;
            }
            if (Character.isDigit(ch)) {
                digit++;
            }
            if (this.specialCharSet.contains(ch)) {
                special++;
            }
        }

        if (upperLetter < this.upperLetter) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects at least " + this.upperLetter
                    + " uppercase letter [A-Z].",
                null);
        }
        if (lowerLetter < this.lowerLetter) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects at least " + this.lowerLetter
                    + " lowercase letter [a-z].",
                null);
        }
        if (letter < this.letter) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects at least " + this.letter
                    + " letters [a-z|A-Z].",
                null);
        }
        if (digit < this.digit) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects at least " + this.digit
                    + " digits [0-9].",
                null);
        }
        if (special <= 0 && this.specialChar) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_PASSWORD,
                "ERR-CODE: [PXC-5203][ERR_INVALID_PASSWORD] Invalid password, expects special char and the special char must be one of '@#$%^&+='.",
                null);
        }

        return true;
    }
}
