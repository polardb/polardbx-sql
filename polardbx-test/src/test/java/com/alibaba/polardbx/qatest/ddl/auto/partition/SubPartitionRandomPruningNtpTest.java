package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.Test;

/**
 * @author chenghui.lch
 */
public class SubPartitionRandomPruningNtpTest extends SubPartitionRandomPruningTpTest {

    public SubPartitionRandomPruningNtpTest() {
        super();
    }

    @Test
    public void runTest() {
        int sqlCnt = 250;
        for (int i = 0; i < sqlCnt; i++) {
            String sql = genRandomSql(false);
            String logMsg = String.format("rngSql[%s]: \n%s;\n\n", i, sql);
            log.info(logMsg);
            runRandomSql(sql);

            String inverseSql = genRandomSql(true);
            logMsg = String.format("inversedRngSql[%s]: \n%s;\n\n", i, inverseSql);
            log.info(logMsg);
            runRandomSql(inverseSql);
        }
    }

    protected String initCreateTableSql(String tbName) {

        /**
         *
         * <pre>
         *
         *
         *
         create table if not exists rnd_sp_pruning_test (
         a bigint not null, 
         b bigint not null, 
         c bigint not null,
         d bigint not null,
         e bigint not null,
         primary key(a,b,c)
         )
         partition by range columns(a,b,c)
         subpartition by range columns(b,c)
         (
         partition p1 values less than (100,1000,10000)
         (
         subpartition p1sp1 values less than (1000,10000),
         subpartition p1sp2 values less than (maxvalue,maxvalue)
         )
         ,
         partition p2 values less than (200,2000,20000)
         (
         subpartition p2sp1 values less than (1000,10000),
         subpartition p2sp2 values less than (2000,20000),
         subpartition p2sp3 values less than (maxvalue,maxvalue)
         )
         ,
         partition p3 values less than (300,3000,30000)
         (
         subpartition p3sp1 values less than (1000,10000),
         subpartition p3sp2 values less than (2000,20000),
         subpartition p3sp3 values less than (3000,30000),
         subpartition p3sp4 values less than (maxvalue,maxvalue)
         )
         ,
         partition p4 values less than (400,4000,40000)
         (
         subpartition p4sp1 values less than (1000,10000),
         subpartition p4sp2 values less than (2000,20000),
         subpartition p4sp3 values less than (3000,30000),
         subpartition p4sp4 values less than (4000,40000),
         subpartition p4sp5 values less than (maxvalue,maxvalue)
         )
         ,
         partition p5 values less than (500,5000,50000)
         (
         subpartition p5sp1 values less than (1000,10000),
         subpartition p5sp2 values less than (2000,20000),
         subpartition p5sp3 values less than (3000,30000),
         subpartition p5sp4 values less than (4000,40000),
         subpartition p5sp5 values less than (5000,50000),
         subpartition p5sp6 values less than (maxvalue,maxvalue)
         )
         ,
         partition p6 values less than (600,6000,60000)
         (
         subpartition p6sp1 values less than (1000,10000),
         subpartition p6sp2 values less than (2000,20000),
         subpartition p6sp3 values less than (3000,30000),
         subpartition p6sp4 values less than (4000,40000),
         subpartition p6sp5 values less than (5000,50000),
         subpartition p6sp6 values less than (6000,60000),
         subpartition p6sp7 values less than (maxvalue,maxvalue)
         )
         ,
         partition p7 values less than (700,7000,70000)
         (
         subpartition p7sp1 values less than (1000,10000),
         subpartition p7sp2 values less than (2000,20000),
         subpartition p7sp3 values less than (3000,30000),
         subpartition p7sp4 values less than (4000,40000),
         subpartition p7sp5 values less than (5000,50000),
         subpartition p7sp6 values less than (6000,60000),
         subpartition p7sp7 values less than (7000,70000),
         subpartition p7sp8 values less than (maxvalue,maxvalue)
         )
         ,
         partition p8 values less than (800,8000,80000)
         (
         subpartition p8sp1 values less than (1000,10000),
         subpartition p8sp2 values less than (2000,20000),
         subpartition p8sp3 values less than (3000,30000),
         subpartition p8sp4 values less than (4000,40000),
         subpartition p8sp5 values less than (5000,50000),
         subpartition p8sp6 values less than (6000,60000),
         subpartition p8sp7 values less than (7000,70000),
         subpartition p8sp8 values less than (8000,80000),
         subpartition p8sp9 values less than (maxvalue,maxvalue)
         )
         ,
         partition p9 values less than (900,9000,90000)
         (
         subpartition p9sp1 values less than (1000,10000),
         subpartition p9sp2 values less than (2000,20000),
         subpartition p9sp3 values less than (3000,30000),
         subpartition p9sp4 values less than (4000,40000),
         subpartition p9sp5 values less than (5000,50000),
         subpartition p9sp6 values less than (6000,60000),
         subpartition p9sp7 values less than (7000,70000),
         subpartition p9sp8 values less than (8000,80000),
         subpartition p9sp9 values less than (9000,90000),
         subpartition p9sp10 values less than (maxvalue,maxvalue)
         )
         ,
         partition p10 values less than (1000,10000,100000)
         (
         subpartition p10sp1 values less than (1000,10000),
         subpartition p10sp2 values less than (2000,20000),
         subpartition p10sp3 values less than (3000,30000),
         subpartition p10sp4 values less than (4000,40000),
         subpartition p10sp5 values less than (5000,50000),
         subpartition p10sp6 values less than (6000,60000),
         subpartition p10sp7 values less than (7000,70000),
         subpartition p10sp8 values less than (8000,80000),
         subpartition p10sp9 values less than (9000,90000),
         subpartition p10sp10 values less than (10000,100000),
         subpartition p10sp11 values less than (maxvalue,maxvalue)
         )
         );
         *
         *
         * </pre>
         *
         *
         *
         */

        String tmpSql = initCreateTableSqlForMySql(tbName)
            + "         partition by range columns(a,b,c)\n"
            + "         subpartition by range columns(b,c)\n"
            + "         (\n"
            + "         partition p1 values less than (100,1000,10000)\n"
            + "         (\n"
            + "         subpartition p1sp1 values less than (1000,10000),\n"
            + "         subpartition p1sp2 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p2 values less than (200,2000,20000)\n"
            + "         (\n"
            + "         subpartition p2sp1 values less than (1000,10000),\n"
            + "         subpartition p2sp2 values less than (2000,20000),\n"
            + "         subpartition p2sp3 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p3 values less than (300,3000,30000)\n"
            + "         (\n"
            + "         subpartition p3sp1 values less than (1000,10000),\n"
            + "         subpartition p3sp2 values less than (2000,20000),\n"
            + "         subpartition p3sp3 values less than (3000,30000),\n"
            + "         subpartition p3sp4 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p4 values less than (400,4000,40000)\n"
            + "         (\n"
            + "         subpartition p4sp1 values less than (1000,10000),\n"
            + "         subpartition p4sp2 values less than (2000,20000),\n"
            + "         subpartition p4sp3 values less than (3000,30000),\n"
            + "         subpartition p4sp4 values less than (4000,40000),\n"
            + "         subpartition p4sp5 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p5 values less than (500,5000,50000)\n"
            + "         (\n"
            + "         subpartition p5sp1 values less than (1000,10000),\n"
            + "         subpartition p5sp2 values less than (2000,20000),\n"
            + "         subpartition p5sp3 values less than (3000,30000),\n"
            + "         subpartition p5sp4 values less than (4000,40000),\n"
            + "         subpartition p5sp5 values less than (5000,50000),\n"
            + "         subpartition p5sp6 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p6 values less than (600,6000,60000)\n"
            + "         (\n"
            + "         subpartition p6sp1 values less than (1000,10000),\n"
            + "         subpartition p6sp2 values less than (2000,20000),\n"
            + "         subpartition p6sp3 values less than (3000,30000),\n"
            + "         subpartition p6sp4 values less than (4000,40000),\n"
            + "         subpartition p6sp5 values less than (5000,50000),\n"
            + "         subpartition p6sp6 values less than (6000,60000),\n"
            + "         subpartition p6sp7 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p7 values less than (700,7000,70000)\n"
            + "         (\n"
            + "         subpartition p7sp1 values less than (1000,10000),\n"
            + "         subpartition p7sp2 values less than (2000,20000),\n"
            + "         subpartition p7sp3 values less than (3000,30000),\n"
            + "         subpartition p7sp4 values less than (4000,40000),\n"
            + "         subpartition p7sp5 values less than (5000,50000),\n"
            + "         subpartition p7sp6 values less than (6000,60000),\n"
            + "         subpartition p7sp7 values less than (7000,70000),\n"
            + "         subpartition p7sp8 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p8 values less than (800,8000,80000)\n"
            + "         (\n"
            + "         subpartition p8sp1 values less than (1000,10000),\n"
            + "         subpartition p8sp2 values less than (2000,20000),\n"
            + "         subpartition p8sp3 values less than (3000,30000),\n"
            + "         subpartition p8sp4 values less than (4000,40000),\n"
            + "         subpartition p8sp5 values less than (5000,50000),\n"
            + "         subpartition p8sp6 values less than (6000,60000),\n"
            + "         subpartition p8sp7 values less than (7000,70000),\n"
            + "         subpartition p8sp8 values less than (8000,80000),\n"
            + "         subpartition p8sp9 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p9 values less than (900,9000,90000)\n"
            + "         (\n"
            + "         subpartition p9sp1 values less than (1000,10000),\n"
            + "         subpartition p9sp2 values less than (2000,20000),\n"
            + "         subpartition p9sp3 values less than (3000,30000),\n"
            + "         subpartition p9sp4 values less than (4000,40000),\n"
            + "         subpartition p9sp5 values less than (5000,50000),\n"
            + "         subpartition p9sp6 values less than (6000,60000),\n"
            + "         subpartition p9sp7 values less than (7000,70000),\n"
            + "         subpartition p9sp8 values less than (8000,80000),\n"
            + "         subpartition p9sp9 values less than (9000,90000),\n"
            + "         subpartition p9sp10 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         ,\n"
            + "         partition p10 values less than (1000,10000,100000)\n"
            + "         (\n"
            + "         subpartition p10sp1 values less than (1000,10000),\n"
            + "         subpartition p10sp2 values less than (2000,20000),\n"
            + "         subpartition p10sp3 values less than (3000,30000),\n"
            + "         subpartition p10sp4 values less than (4000,40000),\n"
            + "         subpartition p10sp5 values less than (5000,50000),\n"
            + "         subpartition p10sp6 values less than (6000,60000),\n"
            + "         subpartition p10sp7 values less than (7000,70000),\n"
            + "         subpartition p10sp8 values less than (8000,80000),\n"
            + "         subpartition p10sp9 values less than (9000,90000),\n"
            + "         subpartition p10sp10 values less than (10000,100000),\n"
            + "         subpartition p10sp11 values less than (maxvalue,maxvalue)\n"
            + "         )\n"
            + "         );";

        return tmpSql;
    }
}
