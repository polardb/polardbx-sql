package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class SequenceNameTest extends DDLBaseNewDBTestCase {

    @Test
    public void testCreateSequenceWithLongSeqName() {
        String sql =
            "create sequence aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggg2 start with 100";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql =
            "alter sequence aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggg2 start with 200";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "drop sequence aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggg2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }
}
