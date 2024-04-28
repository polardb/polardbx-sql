package com.alibaba.polardbx.qatest.columnar.gms;

import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@Ignore
public class ColumnarSchemaManagerTest extends BaseColumnarManagerTest {
    public ColumnarSchemaManagerTest(String caseName, String schemaName, List<String> metaDbSqls) {
        super(caseName, schemaName, metaDbSqls);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        List<Object[]> caseList = new ArrayList<>();
        caseList.add(new Object[] {
            "", "tpch_1g", new ArrayList<>()
        });
        return caseList;
    }

    @Test
    public void testSortKeyColumnMapping() {
        this.columnarManager = new DynamicColumnarManager();
        this.columnarManager.init();

        long tso = columnarManager.latestTso();
        System.out.println(tso);
        System.out.println(columnarManager.getSortKeyColumns(tso, schemaName, "lineitem_col_idx_$9146"));
    }
}
