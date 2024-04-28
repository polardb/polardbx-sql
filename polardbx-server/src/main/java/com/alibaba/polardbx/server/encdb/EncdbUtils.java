package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author pangzhaoxing
 */
public class EncdbUtils {

    /**
     * @param sqlType java.sql.Types
     * @return jdbc protocol type
     */
    public static int sqlType2MysqlType(int sqlType, int scale) {
        int mysqlType;
        if (sqlType != DataType.UNDECIDED_SQL_TYPE) {
            mysqlType = MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(sqlType, scale));
        } else {
            mysqlType = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
        }
        return mysqlType;
    }

}
