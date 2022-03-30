package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLKeywords;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenhui.lch
 */
public class KeyWordsUtil {
    private static Set<String> invalidKeyWords = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    static {
        invalidKeyWords.addAll(MySQLKeywords.DEFAULT_KEYWORDS.getKeywords().keySet());
        invalidKeyWords.add(SystemDbHelper.DEFAULT_DB_NAME);
        invalidKeyWords.add(SystemDbHelper.CDC_DB_NAME);
    }

    public static boolean isKeyWord(String key) {
        if (key == null) {
            return false;
        }
        return invalidKeyWords.contains(key.toUpperCase());
    }
}
