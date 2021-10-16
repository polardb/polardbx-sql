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

package com.alibaba.polardbx.optimizer.parse.bean;

import com.alibaba.polardbx.common.exception.NotSupportException;
import org.apache.commons.lang3.StringUtils;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class NumberParser {
    char[] srcChars;
    char numbers[];
    char decimals[];
    char notations[];
    boolean apport = false;
    boolean notation = false;
    boolean init = false;

    public NumberParser(String chars) {
        if (!StringUtils.isEmpty(chars)) {
            srcChars = chars.toCharArray();
        }
    }

    private void init() {
        if (init) {
            return;
        }
        if (srcChars == null) {
            return;
        }
        init = true;
        StringBuilder sb0 = new StringBuilder(srcChars.length);
        StringBuilder sb1 = new StringBuilder(srcChars.length);
        StringBuilder sb2 = new StringBuilder(srcChars.length);
        for (int i = 0; i < srcChars.length; i++) {
            if ((srcChars[i] >= 48 && srcChars[i] <= 57) || srcChars[i] == 43 || srcChars[i] == 45) {
                if (notation) {
                    sb2.append(srcChars[i]);
                } else if (apport) {
                    sb1.append(srcChars[i]);
                } else {
                    sb0.append(srcChars[i]);
                }
                if (srcChars[i] == 43 || srcChars[i] == 45) {
                    if (notation) {
                        if (sb2.length() != 1) {
                            throw new NotSupportException("Error number");
                        }
                    } else if (apport) {
                        throw new NotSupportException("Error number");
                    } else {
                        if (sb0.length() != 1) {
                            throw new NotSupportException("Error number");
                        }
                    }
                }
            } else if (srcChars[i] == 46) {
                apport = true;
                continue;
            } else if (srcChars[i] == 69 || srcChars[i] == 101) {
                notation = true;
                continue;
            } else {
                throw new NotSupportException("Error number");
            }
        }
        numbers = sb0.toString().toCharArray();
        decimals = sb1.toString().toCharArray();
        notations = sb2.toString().toCharArray();
    }

    public boolean isNotation() {
        init();
        return notation;
    }

}
