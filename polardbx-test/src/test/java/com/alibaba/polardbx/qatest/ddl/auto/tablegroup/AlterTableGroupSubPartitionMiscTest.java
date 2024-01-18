package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

@NotThreadSafe
public class AlterTableGroupSubPartitionMiscTest extends AlterTableGroupTestBase {

    protected static final String LOGICAL_DATABASE = "AlterTableGroupSubPartitionTest";
    protected static final String USE_DATABASE = "use " + LOGICAL_DATABASE;

    public AlterTableGroupSubPartitionMiscTest() {
        super(LOGICAL_DATABASE, ImmutableList.of(ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.name()));
    }

    @Before
    public void setUp() {
        Connection connection = getTddlConnection1();
        reCreateDatabase(connection, LOGICAL_DATABASE);
        JdbcUtil.executeUpdateSuccess(connection, USE_DATABASE);
    }

    @Test
    public void testAddSubPartWithCaseSensitiveName() {
        String tableName = "zmf";

        String sql = "CREATE TABLE `" + tableName + "` (\n"
            + "    `G` mediumint(5) UNSIGNED ZEROFILL DEFAULT NULL COMMENT 'U9x',\n"
            + "    `2Ws` char(1) DEFAULT NULL COMMENT 'xx',\n"
            + "    `fpeR` int(10) UNSIGNED ZEROFILL NOT NULL COMMENT 'C',\n"
            + "    `5j` datetime NOT NULL,\n"
            + "    `N1SY` bigint(20) UNSIGNED ZEROFILL NOT NULL COMMENT '2p',\n"
            + "    `v50` bigint(20) UNSIGNED ZEROFILL NOT NULL,\n"
            + "    `hd` int(10) UNSIGNED NOT NULL,\n"
            + "    `hFMl` date DEFAULT NULL COMMENT 'A',\n"
            + "    `1` date DEFAULT NULL,\n"
            + "    `f5x` char(20) DEFAULT NULL COMMENT 'omb5',\n"
            + "    PRIMARY KEY (`5j`),\n"
            + "    KEY `O` USING BTREE (`N1SY`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE COLUMNS(`f5x`,`1`)\n"
            + "SUBPARTITION BY LIST COLUMNS(`hd`,`5j`)\n"
            + "(SUBPARTITION `zpj` VALUES IN ((265180274,'2061-11-08 00:00:00'),(492739763,'2030-07-28 00:00:00'),(1186267463,'2072-07-28 00:00:00')),\n"
            + "SUBPARTITION `uc` VALUES IN ((1304693641,'2094-12-08 00:00:00'),(1496579062,'1984-06-02 00:00:00'),(725152130,'2103-09-21 00:00:00')))\n"
            + "(PARTITION `p1` VALUES LESS THAN ('A','1990-01-01'),\n"
            + "PARTITION `p2` VALUES LESS THAN ('A','1991-07-30'),\n"
            + "PARTITION `d` VALUES LESS THAN ('G','2075-06-11'),\n"
            + "PARTITION `e` VALUES LESS THAN ('H','2075-06-11'))";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "ALTER TABLE `" + tableName + "` ADD SUBPARTITION "
            + "( SUBPARTITION `kpkZPPhcsQ` VALUES IN ( ('489710691','2090-05-19'),('658738620','2116-06-19'),('225326585','2017-07-26') ) )";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        dropTableIfExists(tableName);
    }

    @Test
    public void testAddPartWithSubPart() {
        String tableName = "shetgwawq7twac";

        String sql = "CREATE TABLE `shetgwawq7twac` (\n"
            + "    `OSmn65A4NvXJ` date DEFAULT NULL COMMENT 'ORPpzrcDVF',\n"
            + "    `Su` int(5) NOT NULL,\n"
            + "    `yOyWDby7xqPGb` smallint(5) UNSIGNED NOT NULL COMMENT 'N',\n"
            + "    `wSudmlsc1HSO2WN` mediumint(2) UNSIGNED ZEROFILL NOT NULL,\n"
            + "    `OUQutN5pG5` smallint(1) UNSIGNED ZEROFILL NOT NULL AUTO_INCREMENT COMMENT 'e8',\n"
            + "    `vrs460O1` int(4) UNSIGNED ZEROFILL NOT NULL COMMENT '4aEclu',\n"
            + "    `2oI` datetime NOT NULL,\n"
            + "    `h8Rz9Yx` int(10) UNSIGNED DEFAULT NULL,\n"
            + "    `IPIU` char(23) NOT NULL,\n"
            + "    `b` mediumint(7) DEFAULT NULL COMMENT 'Op5mxNYiwPh',\n"
            + "    `4U7Yu7LDr` bigint(20) UNSIGNED ZEROFILL DEFAULT NULL,\n"
            + "    `T1vj3KsIdYwQ` int(2) UNSIGNED ZEROFILL DEFAULT NULL,\n"
            + "    `S1OPdcqBMGjGU` datetime DEFAULT NULL COMMENT 'u',\n"
            + "    `Y8xZ` int(5) UNSIGNED ZEROFILL NOT NULL COMMENT '6I8b7YmoSNS',\n"
            + "    `2zQbXR89x` bigint(1) NOT NULL COMMENT 'Bg',\n"
            + "    `GYzzSFaJLerJZ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'rzkn8tqw8lTQ',\n"
            + "    `6q7uQSc` char(28) DEFAULT NULL,\n"
            + "    `35` datetime DEFAULT NULL,\n"
            + "    `Y05` date NOT NULL,\n"
            + "    `g` date DEFAULT NULL,\n"
            + "    `5yVDcaaP2MTlx` bigint(20) UNSIGNED ZEROFILL DEFAULT NULL,\n"
            + "    `22B` datetime DEFAULT NULL,\n"
            + "    `Bhb` bigint(20) UNSIGNED ZEROFILL NOT NULL,\n"
            + "    PRIMARY KEY (`OUQutN5pG5`),\n"
            + "    UNIQUE KEY `yOyWDby7xqPGb` (`yOyWDby7xqPGb`),\n"
            + "    UNIQUE KEY `wSudmlsc1HSO2WN` (`wSudmlsc1HSO2WN`),\n"
            + "    UNIQUE KEY `OUQutN5pG5` (`OUQutN5pG5`),\n"
            + "    UNIQUE KEY `6q7uQSc` (`6q7uQSc`),\n"
            + "    KEY `3dtRgpq` (`Su`, `6q7uQSc`, `S1OPdcqBMGjGU`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE COLUMNS(`2oI`,`2zQbXR89x`)\n"
            + "SUBPARTITION BY LIST COLUMNS(`2zQbXR89x`,`Bhb`)"
            + "(PARTITION `nzduuvtz8wzd` VALUES LESS THAN ('1993-12-18 00:00:00',1466144368)\n"
            + "(SUBPARTITION `s2ekrhczmfr` VALUES IN ((1214099242,1616297013),(307960098,863489613)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `kq5aukcoql` VALUES IN ((1551623801,1850031945),(1206949730,504305818)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `7rg` VALUES IN ((1623312732,168166482),(1171484440,595212540)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `uxbv1gec` VALUES IN ((20583402,209388910),(285537138,1474364882)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `e4lwthyrfxdoo5n` VALUES IN ((314779339,1679215397),(1172128269,2106517015)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `s` VALUES IN ((426464672,815618757),(1630275797,586608136)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `s4qthmnhks2hfe` VALUES IN ((888596740,1421311773),(1531625109,1779979502),(701135574,1309355971)) ENGINE = InnoDB),\n"
            + "PARTITION `t5ob9lltw` VALUES LESS THAN ('2036-07-09 00:00:00',1680751885)\n"
            + "(SUBPARTITION `owu7catvoc` VALUES IN ((12463778,1112910168),(787849622,1744334081),(167555236,765934950)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `yz7opj6f` VALUES IN ((130247717,160656098),(409754922,1919349300),(798858964,1789921631)) ENGINE = InnoDB,\n"
            + " SUBPARTITION `hvbt` VALUES IN ((1642473552,698288471),(822120358,2110668937)) ENGINE = InnoDB))";

        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "ALTER TABLE `shetgwawq7twac` ADD PARTITION (\n"
            + "    PARTITION `KqI4G` VALUES LESS THAN ( '2010-12-27',278941253 ) SUBPARTITIONS 1 (\n"
            + "        SUBPARTITION `lgNTk8f` VALUES IN ( (1811452433,683402186),(781169472,178951742) )\n"
            + "    ),\n"
            + "    PARTITION `qG` VALUES LESS THAN ( '2029-12-11',489852502 ) SUBPARTITIONS 1 (\n"
            + "        SUBPARTITION `2cMOn` VALUES IN ( (1507505806,1020747226),(1587800357,2109107152),(867124648,402387488) )\n"
            + "    ),\n"
            + "    PARTITION `PK23wxZ` VALUES LESS THAN ( '2071-09-27',1209760232 ) SUBPARTITIONS 2 (\n"
            + "        SUBPARTITION `A3oC` VALUES IN ( (1532929965,683506035),(700293477,270412745),(434969271,1128661726) ),\n"
            + "        SUBPARTITION `Xo37VH57iQEx` VALUES IN ( (2124924128,888430060),(816078365,485271163),(1084712840,1854057996) )\n"
            + "    )\n"
            + ");";

        JdbcUtil.executeUpdateFailed(tddlConnection, sql,
            "the new partition value[('2010-12-27 00:00:00',278941253)] must greater than other partitions");

        dropTableIfExists(tableName);
    }

}
