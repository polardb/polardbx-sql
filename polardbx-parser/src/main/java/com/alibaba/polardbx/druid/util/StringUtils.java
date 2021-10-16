/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.util;

import com.alibaba.polardbx.druid.support.logging.Log;
import com.alibaba.polardbx.druid.support.logging.LogFactory;

/**
 * @author sandzhang[sandzhangtoo@gmail.com]
 */
public class StringUtils {

    private final static Log LOG = LogFactory.getLog(StringUtils.class);

    public static String removeNameQuotes(String s) {
        if (s == null || s.length() <= 1) {
            return null;
        }
        int len = s.length();
        char c0 = s.charAt(0);
        char last = s.charAt(len - 1);

        if (c0 == last && (c0 == '`' || c0 == '\'' || c0 == '\"') ) {
            return s.substring(1, len - 1);
        }
        return s;
    }

    public static String quoteAlias(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }

        char firstChar = name.charAt(0);
        if (firstChar >= 48 && firstChar <= 57) {
            return name = "\"" + name + "\"";
        }

        if (name.length() > 2 && firstChar == '`' && name.charAt(name.length() - 1) == '`') {
            String str = name.substring(1, name.length() -1);
            str = str.replaceAll("\\\"", "\\\"\\\"");
            return '"' + str + '"';
        } else if (name.length() > 2 && firstChar == '\'' && name.charAt(name.length() - 1) == '\'') {
            if (name.indexOf('"') != -1) {
                String str = name.substring(1, name.length() -1);
                str = str.replaceAll("\\\"", "\\\"\\\"");
                return '"' + str + '"';
            } else {
                char[] chars = name.toCharArray();
                chars[0] = '"';
                chars[chars.length - 1] = '"';
                return new String(chars);
            }
        } else if (name.length() == 2 && firstChar == '\'' && name.charAt(1) == '\'') {
            return "\"\"";
        } else if (name.length() > 0 && firstChar != '"') {
            for (int i = 0; i < name.length(); ++i) {
                boolean unicode = false;
                char ch = name.charAt(i);
                if (ch > 128) {
                    unicode = true;
                }
                if (unicode) {
                    String name2 = '"' + name + '"';
                    return name2;
                }
            }
        }

        return name;
    }

    public static String backquoteAlias(String name) {
        if (name == null || name.length() == 0) {
            return name;
        }

        char firstChar = name.charAt(0);
        if (firstChar >= 48 && firstChar <= 57) {
            return "`" + name + "`";
        }

        if (name.length() > 2 && firstChar == '"' && name.charAt(name.length() - 1) == '"') {
            String str = name.substring(1, name.length() -1);
            str = str.replaceAll("\\\"", "\\\"\\\"");
            return '`' + str + '`';
        } else if (name.length() > 2 && firstChar == '\'' && name.charAt(name.length() - 1) == '\'') {
            if (name.indexOf('`') != -1) {
                String str = name.substring(1, name.length() -1);
                str = str.replaceAll("\\`", "\\`\\`");
                return '`' + str + '`';
            } else {
                char[] chars = name.toCharArray();
                chars[0] = '`';
                chars[chars.length - 1] = '`';
                return new String(chars);
            }
        } else if (name.length() == 2 && firstChar == '\'' && name.charAt(1) == '\'') {
            return "``";
        } else if (name.length() > 0 && firstChar != '`') {
            for (int i = 0; i < name.length(); ++i) {
                boolean unicode = false;
                char ch = name.charAt(i);
                if (ch > 128) {
                    unicode = true;
                }
                if (unicode) {
                    String name2 = '`' + name + '`';
                    return name2;
                }
            }
        }

        return name;
    }

    /**
     * Example: subString("12345","1","4")=23
     * 
     * @param src
     * @param start
     * @param to
     * @return
     */
    public static Integer subStringToInteger(String src, String start, String to) {
        return stringToInteger(subString(src, start, to));
    }

    /**
     * Example: subString("abcd","a","c")="b"
     * 
     * @param src
     * @param start null while start from index=0
     * @param to null while to index=src.length
     * @return
     */
    public static String subString(String src, String start, String to) {
        int indexFrom = start == null ? 0 : src.indexOf(start);
        int indexTo = to == null ? src.length() : src.indexOf(to);
        if (indexFrom < 0 || indexTo < 0 || indexFrom > indexTo) {
            return null;
        }

        if (null != start) {
            indexFrom += start.length();
        }

        return src.substring(indexFrom, indexTo);

    }

    /**
     * Example: subString("abcdc","a","c",true)="bcd"
     * 
     * @param src
     * @param start null while start from index=0
     * @param to null while to index=src.length
     * @param toLast true while to index=src.lastIndexOf(to)
     * @return
     */
    public static String subString(String src, String start, String to, boolean toLast) {
        if(!toLast) {
            return subString(src, start, to);
        }
        int indexFrom = start == null ? 0 : src.indexOf(start);
        int indexTo = to == null ? src.length() : src.lastIndexOf(to);
        if (indexFrom < 0 || indexTo < 0 || indexFrom > indexTo) {
            return null;
        }

        if (null != start) {
            indexFrom += start.length();
        }

        return src.substring(indexFrom, indexTo);

    }

    /**
     * @param in
     * @return
     */
    public static Integer stringToInteger(String in) {
        if (in == null) {
            return null;
        }
        in = in.trim();
        if (in.length() == 0) {
            return null;
        }
        
        try {
            return Integer.parseInt(in);
        } catch (NumberFormatException e) {
            LOG.warn("stringToInteger fail,string=" + in, e);
            return null;
        }
    }

    public static boolean equals(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equals(b);
    }
    
    public static boolean equalsIgnoreCase(String a, String b) {
        if (a == null) {
            return b == null;
        }
        return a.equalsIgnoreCase(b);
    }

    public static boolean isEmpty(String value) {
        return isEmpty((CharSequence) value);
    }

    public static boolean isEmpty(CharSequence value) {
        if (value == null || value.length() == 0) {
            return true;
        }

        return false;
    }
    
    public static int lowerHashCode(String text) {
        if (text == null) {
            return 0;
        }
//        return text.toLowerCase().hashCode();
        int h = 0;
        for (int i = 0; i < text.length(); ++i) {
            char ch = text.charAt(i);
            if (ch >= 'A' && ch <= 'Z') {
                ch = (char) (ch + 32);
            }

            h = 31 * h + ch;
        }
        return h;
    }

    public static boolean isNumber(String str) {
        if (str.length() == 0) {
            return false;
        }
        int sz = str.length();
        boolean hasExp = false;
        boolean hasDecPoint = false;
        boolean allowSigns = false;
        boolean foundDigit = false;
        // deal with any possible sign up front
        int start = (str.charAt(0) == '-') ? 1 : 0;
        if (sz > start + 1) {
            if (str.charAt(start) == '0' && str.charAt(start + 1) == 'x') {
                int i = start + 2;
                if (i == sz) {
                    return false; // str == "0x"
                }
                // checking hex (it can't be anything else)
                for (; i < str.length(); i++) {
                    char ch = str.charAt(i);
                    if ((ch < '0' || ch > '9')
                            && (ch < 'a' || ch > 'f')
                            && (ch < 'A' || ch > 'F')) {
                        return false;
                    }
                }
                return true;
            }
        }
        sz--; // don't want to loop to the last char, check it afterwords
        // for type qualifiers
        int i = start;
        // loop to the next to last char or to the last char if we need another digit to
        // make a valid number (e.g. chars[0..5] = "1234E")
        while (i < sz || (i < sz + 1 && allowSigns && !foundDigit)) {
            char ch = str.charAt(i);
            if (ch >= '0' && ch <= '9') {
                foundDigit = true;
                allowSigns = false;

            } else if (ch == '.') {
                if (hasDecPoint || hasExp) {
                    // two decimal points or dec in exponent
                    return false;
                }
                hasDecPoint = true;
            } else if (ch == 'e' || ch == 'E') {
                // we've already taken care of hex.
                if (hasExp) {
                    // two E's
                    return false;
                }
                if (!foundDigit) {
                    return false;
                }
                hasExp = true;
                allowSigns = true;
            } else if (ch == '+' || ch == '-') {
                if (!allowSigns) {
                    return false;
                }
                allowSigns = false;
                foundDigit = false; // we need a digit after the E
            } else {
                return false;
            }
            i++;
        }
        if (i < str.length()) {
            char ch = str.charAt(i);

            if (ch >= '0' && ch <= '9') {
                // no type qualifier, OK
                return true;
            }
            if (ch == 'e' || ch == 'E') {
                // can't have an E at the last byte
                return false;
            }
            if (!allowSigns
                    && (ch == 'd'
                    || ch == 'D'
                    || ch == 'f'
                    || ch == 'F')) {
                return foundDigit;
            }
            if (ch == 'l'
                    || ch == 'L') {
                // not allowing L with an exponent
                return foundDigit && !hasExp;
            }
            // last character is illegal
            return false;
        }
        // allowSigns is true iff the val ends in 'E'
        // found digit it to make sure weird stuff like '.' and '1E-' doesn't pass
        return !allowSigns && foundDigit;
    }

    public static boolean isNumber(char[] chars) {
        if (chars.length == 0) {
            return false;
        }
        int sz = chars.length;
        boolean hasExp = false;
        boolean hasDecPoint = false;
        boolean allowSigns = false;
        boolean foundDigit = false;
        // deal with any possible sign up front
        int start = (chars[0] == '-') ? 1 : 0;
        if (sz > start + 1) {
            if (chars[start] == '0' && chars[start + 1] == 'x') {
                int i = start + 2;
                if (i == sz) {
                    return false; // str == "0x"
                }
                // checking hex (it can't be anything else)
                for (; i < chars.length; i++) {
                    char ch = chars[i];
                    if ((ch < '0' || ch > '9')
                            && (ch < 'a' || ch > 'f')
                            && (ch < 'A' || ch > 'F')) {
                        return false;
                    }
                }
                return true;
            }
        }
        sz--; // don't want to loop to the last char, check it afterwords
        // for type qualifiers
        int i = start;
        // loop to the next to last char or to the last char if we need another digit to
        // make a valid number (e.g. chars[0..5] = "1234E")
        while (i < sz || (i < sz + 1 && allowSigns && !foundDigit)) {
            char ch = chars[i];
            if (ch >= '0' && ch <= '9') {
                foundDigit = true;
                allowSigns = false;

            } else if (ch == '.') {
                if (hasDecPoint || hasExp) {
                    // two decimal points or dec in exponent
                    return false;
                }
                hasDecPoint = true;
            } else if (ch == 'e' || ch == 'E') {
                // we've already taken care of hex.
                if (hasExp) {
                    // two E's
                    return false;
                }
                if (!foundDigit) {
                    return false;
                }
                hasExp = true;
                allowSigns = true;
            } else if (ch == '+' || ch == '-') {
                if (!allowSigns) {
                    return false;
                }
                allowSigns = false;
                foundDigit = false; // we need a digit after the E
            } else {
                return false;
            }
            i++;
        }
        if (i < chars.length) {
            char ch = chars[i];
            if (ch >= '0' && ch <= '9') {
                // no type qualifier, OK
                return true;
            }
            if (ch == 'e' || ch == 'E') {
                // can't have an E at the last byte
                return false;
            }
            if (!allowSigns
                    && (ch == 'd'
                    || ch == 'D'
                    || ch == 'f'
                    || ch == 'F')) {
                return foundDigit;
            }
            if (ch == 'l'
                    || ch == 'L') {
                // not allowing L with an exponent
                return foundDigit && !hasExp;
            }

            if (ch == '.') {
                return true;
            }
            // last character is illegal
            return false;
        }
        // allowSigns is true iff the val ends in 'E'
        // found digit it to make sure weird stuff like '.' and '1E-' doesn't pass
        return !allowSigns && foundDigit;
    }

    public static String rtrim(String val) {
        if (val == null) {
            return null;
        }

        int len = val.length();
//        char[] val = new char[value.length()];    /* avoid getfield opcode */

//        while ((st < len) && (val.charAt(st) <= ' ')) {
//            st++;
//        }
        while (val.charAt(len - 1) <= ' ') {
            len--;
        }
        return (len < val.length()) ? val.substring(0, len) : val;
    }

    public static String ltrim(String val) {
        if (val == null) {
            return null;
        }

        int len = val.length();
        int st = 0;
        while ((st < len) && (val.charAt(st) <= ' ')) {
            st++;
        }

        return (st > 0) ? val.substring(st, len) : val;
    }
}
