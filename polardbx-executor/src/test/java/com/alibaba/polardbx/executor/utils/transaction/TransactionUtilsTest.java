package com.alibaba.polardbx.executor.utils.transaction;

import com.alibaba.polardbx.common.mock.MockStatus;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionUtilsTest {
    @Test
    public void testUpdateLookupSet() {
        MockStatus.setMock(true);
        List<List<Map<String, Object>>> results = new ArrayList<>();
        Map<String, Object> row0 = new HashMap<>();
        row0.put("TRANS_ID", 100001L);
        row0.put("GROUP", "test_group0");
        row0.put("CONN_ID", 1024L);
        row0.put("FRONTEND_CONN_ID", 10240L);
        row0.put("START_TIME", System.currentTimeMillis());
        row0.put("SQL", "SELECT * FROM TB1 FOR UPDATE");
        row0.put("DDL", true);
        Map<String, Object> row1 = new HashMap<>();
        row1.put("TRANS_ID", 100002L);
        row1.put("GROUP", "test_group1");
        row1.put("CONN_ID", 1025L);
        row1.put("FRONTEND_CONN_ID", 10250L);
        row1.put("START_TIME", System.currentTimeMillis());
        row1.put("SQL", "SELECT * FROM TB1 FOR UPDATE");
        results.add(ImmutableList.of(row0, row1));
        results.add(null);
        TrxLookupSet lookupSet = new TrxLookupSet();
        TransactionUtils.updateTrxLookupSet(results, lookupSet);
        Assert.assertTrue(lookupSet.getTransaction(100001L).isDdl());
        Assert.assertFalse(lookupSet.getTransaction(100002L).isDdl());
        System.out.println(lookupSet);
    }
}
