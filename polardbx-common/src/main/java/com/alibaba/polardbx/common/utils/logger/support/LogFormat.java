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

package com.alibaba.polardbx.common.utils.logger.support;

public class LogFormat {


    public static String formatLog(String sql) {
        char lastQuote = '\0';
        boolean hasSpace = true;
        boolean hasEscape = false;
        boolean multiLineComment = false;
        boolean inlineComment = false;

        StringBuilder r = new StringBuilder(sql.length());

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (inlineComment) {
                if (c == '\n' || c == '\r') {
                    inlineComment = false;
                    r.append("*/");
                } else if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
                    r.append("**");
                    i += 1;
                } else {
                    r.append(c);
                }
                continue;
            }

            outer:
            switch (c) {
            case '\\':
                if (lastQuote == '\0') {
                    r.append(c);
                } else if (hasEscape) {
                    r.append("\\\\");
                }
                hasSpace = false;
                hasEscape = !hasEscape;
                break;
            case '\'':
            case '\"':
                hasSpace = false;
                if (!hasEscape) {
                    if (lastQuote == c) {
                        lastQuote = '\0';
                        r.append(c);
                    } else if (lastQuote == '\0') {
                        lastQuote = c;
                        r.append(c);
                    } else {
                        if (c == '\'') {
                            r.append("'");
                        } else if (c == '\"') {
                            r.append("\"");
                        }
                    }
                } else {
                    r.append("\\").append(c);
                }
                hasEscape = false;
                break;
            case '\r':
            case '\t':
            case '\n':
            case '\b':
            case '\0':
            case ' ':
                if (lastQuote != '\0') {
                    if (c == '\r') {
                        r.append("\\r");
                    } else if (c == '\t') {
                        r.append("\\t");
                    } else if (c == '\n') {
                        r.append("\\n");
                    } else if (c == '\b') {
                        r.append("\\b");
                    } else if (c == '\0') {
                        r.append("\\0");
                    } else if (c == ' ') {
                        r.append(' ');
                    }
                } else if (!hasSpace) {
                    r.append(' ');
                    hasSpace = true;
                }
                hasEscape = false;
                break;
            default:
                if (lastQuote == 0) {
                    switch (c) {
                    case '#':
                        if (!multiLineComment) {
                            inlineComment = true;
                            r.append("/*");
                            break outer;
                        }
                        break;
                    case '-':
                        if (!multiLineComment) {
                            if (i + 2 < sql.length() && sql.charAt(i + 1) == '-' && Character
                                .isWhitespace(sql.charAt(i + 2))) {
                                i += 1; // don't include the whitespace character since it may be a newline
                                inlineComment = true;
                                r.append("/*");
                                break outer;
                            }
                        }
                        break;
                    case '*':
                        if (multiLineComment && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
                            multiLineComment = false;
                        } else if (i - 1 >= 0 && sql.charAt(i - 1) == '/') {
                            multiLineComment = true;
                        }
                        break;
                    }
                }
                if (lastQuote != '\0' && hasEscape) {
                    r.append('\\');
                }
                r.append(c);
                hasSpace = false;
                hasEscape = false;
                break;
            }
        }

        if (inlineComment) {
            r.append("*/");
        }
        return r.toString();
    }
}
