package com.alibaba.polardbx.metadb;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class InfoSchemaCommonTest {
    public static final Set<String> TABLES_STAT_NAMES = new HashSet<>();

    @Before
    public void prepare() {
        TABLES_STAT_NAMES.add("Rows");
        TABLES_STAT_NAMES.add("Avg_row_length");
        TABLES_STAT_NAMES.add("Data_length");
        TABLES_STAT_NAMES.add("Max_data_length");
        TABLES_STAT_NAMES.add("Index_length");
        TABLES_STAT_NAMES.add("Auto_increment");
    }

    @Test
    public void infoSchemaCommonTest() {
        InfoSchemaCommon infoSchemaCommon = new InfoSchemaCommon();
        long num = 0;
        for (int i = 0; i < SqlShowTableStatus.NUM_OF_COLUMNS; i++) {
            String columnName = SqlShowTableStatus.COLUMN_NAMES.get(i);
            if (TABLES_STAT_NAMES.contains(columnName)) {
                num++;
            }
        }
        Assert.assertTrue(infoSchemaCommon.TABLES_STAT_INDEXES.size() == num);
    }
}
