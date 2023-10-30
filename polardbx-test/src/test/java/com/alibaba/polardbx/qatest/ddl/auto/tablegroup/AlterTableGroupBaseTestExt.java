package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableGroupBaseTestExt extends AlterTableGroupTestBase {

    public AlterTableGroupBaseTestExt(String logicalDatabase, List<String> finalTableStatus) {
        super(logicalDatabase, finalTableStatus);
    }

    public static final String PART_BY_KEY = "partition by key (id)";
    public static final String PART_BY_RANGE = "partition by range (year(c_datetime))";
    public static final String PART_BY_RANGE_COLUMNS = "partition by range columns (c_datetime,c_int_32)";
    public static final String PART_BY_LIST = "partition by list (id)";
    public static final String PART_BY_LIST_COLUMNS = "partition by list columns (id,c_varchar)";

    public static final String PART_NUM = "partitions %s";
    public static final String PART_RANGE = "partition %s values less than (%s)";
    public static final String PART_LIST = "partition %s values in (%s)";

    public static final String SUBPART_BY_KEY = "subpartition by key (c_int_32)";
    public static final String SUBPART_BY_RANGE = "subpartition by range (year(c_datetime))";
    public static final String SUBPART_BY_RANGE_COLUMNS = "subpartition by range columns (id,c_datetime)";
    public static final String SUBPART_BY_LIST = "subpartition by list (c_int_32)";
    public static final String SUBPART_BY_LIST_COLUMNS = "subpartition by list columns (c_int_32,c_char)";

    public static final String SUBPART_NUM = "subpartitions %s";
    public static final String SUBPART_RANGE = "subpartition %s values less than (%s)";
    public static final String SUBPART_LIST = "subpartition %s values in (%s)";

    /**
     * Template
     */

    public static final String T_PART_RANGE =
        String.format(T_PART_OR_SUBPART(PART_RANGE),
            "p1", "2001",
            "p2", "2012",
            "p3", "2023"
        );
    public static final String T_PART_RANGE_COLUMNS =
        String.format(T_PART_OR_SUBPART(PART_RANGE),
            "p1", "'2011-01-01 00:00:00',11",
            "p2", "'2012-01-01 00:00:00',21",
            "p3", "'2013-01-01 00:00:00',31"
        );
    public static final String T_PART_LIST =
        String.format(T_PART_OR_SUBPART(PART_LIST),
            "p1", "1, 10, 100",
            "p2", "2, 20, 200",
            "p3", "3, 30, 300"
        );
    public static final String T_PART_LIST_COLUMNS =
        String.format(T_PART_OR_SUBPART(PART_LIST),
            "p1", "(1,'abc1'), (10,'abc1'), (100,'abc1')",
            "p2", "(2,'abc2'), (20,'abc2'), (200,'abc2')",
            "p3", "(3,'abc3'), (30,'abc3'), (300,'abc3')"
        );

    public static final String T_SUBPART_RANGE =
        String.format(T_PART_OR_SUBPART(SUBPART_RANGE),
            "sp1", "2001",
            "sp2", "2012",
            "sp3", "2023"
        );
    public static final String T_SUBPART_RANGE_COLUMNS =
        String.format(T_PART_OR_SUBPART(SUBPART_RANGE),
            "sp1", "10,'2001-01-01 00:00:00'",
            "sp2", "20,'2012-01-01 00:00:00'",
            "sp3", "30,'2023-01-01 00:00:00'"
        );
    public static final String T_SUBPART_LIST =
        String.format(T_PART_OR_SUBPART(SUBPART_LIST),
            "sp1", "11, 12, 13",
            "sp2", "21, 22, 23",
            "sp3", "31, 32, 33"
        );
    public static final String T_SUBPART_LIST_COLUMNS =
        String.format(T_PART_OR_SUBPART(SUBPART_LIST),
            "sp1", "(11,'def1'), (12,'def1'), (13,'def1')",
            "sp2", "(21,'def2'), (22,'def1'), (23,'def1')",
            "sp3", "(31,'def3'), (32,'def1'), (33,'def1')"
        );

    /**
     * Non-Template
     */

    // PARTITION BY RANGE

    public static final String NT_PART_RANGE_SUBPART_KEY =
        String.format(NT_PART_WITH_SUBPART_KEY(PART_RANGE),
            "2001",
            "2012",
            "2023"
        );

    public static final String SPLIT_NOT_HASH_PART(String template, boolean isSplitSubPart) {
        String type = isSplitSubPart ? "subpartition" : "partition";
        return "split " + type + " %s into (\n"
            + PART(template, !isSplitSubPart ? "p20" : "sp200", "%s", "%s", false)
            + PART(template, !isSplitSubPart ? "p21" : "sp201", "%s", "%s", false)
            + PART(template, !isSplitSubPart ? "p22" : "sp202", "%s", "%s", true)
            + ")";
    }

    ;

    public static final String NT_PART_RANGE_SUBPART_KEY_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "2008", "",
            "2010", "(SUBPARTITION p21sp100, SUBPARTITION p21sp101, SUBPARTITION p21sp102)",
            "2012", "");

    public static final String NT_RANGE_SUBPART_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_RANGE, true), "p2sp2",
            "2008", "",
            "2010", "",
            "2012", "");

    public static final String NT_KEY_SUBPART_SPLIT_SUB_PART = "split subpartition p2sp2";

    public static final String NT_PART_RANGE_SUBPART_RANGE =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_RANGE),
            "2001", "2001",
            "2012", "2001", "2012",
            "2023", "2001", "2012", "2023"
        );

    public static final String NT_PART_RANGE_SUBPART_RANGE_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "2008", "",
            "2010",
            "(SUBPARTITION p21sp100 values less than(2010), SUBPARTITION p21sp101 values less than(2015), SUBPARTITION p21sp102 values less than(2022))",
            "2012", "");

    public static final String NT_PART_RANGE_SUBPART_RANGE_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_RANGE),
            "2001", "10,'2011-01-01 00:00:00'",
            "2012", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "2023", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'", "30,'2013-01-01 00:00:00'"
        );

    public static final String NT_PART_RANGE_SUBPART_RANGE_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "2008", "",
            "2010",
            "(SUBPARTITION p21sp100 values less than(0,'2011-01-01 00:00:00'), SUBPARTITION p21sp101 values less than(15,'2011-01-01 00:00:00'), SUBPARTITION p21sp102 values less than(20,'2011-01-01 00:00:00'))",
            "2012", "");

    public static final String NT_RANGE_COLUMNS_SUBPART_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_RANGE, true), "p2sp2",
            "12,'2011-01-01 00:00:00'", "",
            "15,'2011-01-01 00:00:00'", "",
            "20,'2012-01-01 00:00:00'", "");

    public static final String NT_PART_RANGE_SUBPART_LIST =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_LIST),
            "2001", "11, 12, 13",
            "2012", "11, 12, 13", "21, 22, 23",
            "2023", "11, 12, 13", "21, 22, 23", "31, 32, 33"
        );

    public static final String NT_PART_RANGE_SUBPART_LIST_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "2008", "",
            "2010",
            "(SUBPARTITION p21sp100 values in (11,21), SUBPARTITION p21sp101 values in (12,22), SUBPARTITION p21sp102 values in(13,23))",
            "2012", "");

    public static final String NT_PART_LIST_SUBPART_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_LIST, true), "p2sp2",
            "21", "",
            "22", "",
            "23", "");

    public static final String NT_PART_RANGE_SUBPART_LIST_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_LIST),
            "2001", "(11,'def1'), (12,'def1'), (13,'def1')",
            "2012", "(11,'def1'), (12,'def1'), (13,'def1')", "(21,'def2'), (22,'def2'), (23,'def2')",
            "2023", "(11,'def1'), (12,'def1'), (13,'def1')", "(21,'def2'), (22,'def2'), (23,'def2')",
            "(31,'def3'), (32,'def3'), (33,'def3')"
        );

    public static final String NT_PART_RANGE_SUBPART_LIST_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "2008", "",
            "2010",
            "(SUBPARTITION p21sp100 values in ((11,'def1'),(21,'def2')), SUBPARTITION p21sp101 values in ((12,'def1'),(22,'def2')), SUBPARTITION p21sp102 values in((13,'def1'),(23,'def2')))",
            "2012", "");

    public static final String NT_PART_LIST_COLUMNS_SUBPART_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_LIST, true), "p2sp2",
            "(21,'def2')", "",
            "(23,'def2')", "",
            "(22,'def2')", "");

    // PARTITION BY RANGE COLUMNS

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_KEY =
        String.format(NT_PART_WITH_SUBPART_KEY(PART_RANGE),
            "'2001-01-01 00:00:00',11",
            "'2012-01-01 00:00:00',21",
            "'2023-01-01 00:00:00',31"
        );

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_KEY_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00',15", "",
            "'2012-01-01 00:00:00',18", "(SUBPARTITION p21sp100, SUBPARTITION p21sp101, SUBPARTITION p21sp102)",
            "'2012-01-01 00:00:00',21", "");

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_RANGE =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_RANGE),
            "'2001-01-01 00:00:00',11", "2001",
            "'2012-01-01 00:00:00',21", "2001", "2012",
            "'2023-01-01 00:00:00',31", "2001", "2012", "2023"
        );

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_RANGE_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00',15", "",
            "'2012-01-01 00:00:00',18",
            "(SUBPARTITION p21sp100 values less than(2010), SUBPARTITION p21sp101 values less than(2015), SUBPARTITION p21sp102 values less than(2022))",
            "'2012-01-01 00:00:00',21", "");

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_RANGE_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_RANGE),
            "'2001-01-01 00:00:00',11", "10,'2011-01-01 00:00:00'",
            "'2012-01-01 00:00:00',21", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "'2023-01-01 00:00:00',31", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "31,'2013-01-01 00:00:00'"
        );

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_RANGE_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00',15", "",
            "'2012-01-01 00:00:00',18",
            "(SUBPARTITION p21sp100 values less than(0,'2011-01-01 00:00:00'), SUBPARTITION p21sp101 values less than(15,'2011-01-01 00:00:00'), SUBPARTITION p21sp102 values less than(20,'2011-01-01 00:00:00'))",
            "'2012-01-01 00:00:00',21", "");

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_LIST =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_LIST),
            "'2001-01-01 00:00:00',11", "11, 12, 13",
            "'2012-01-01 00:00:00',21", "11, 12, 13", "21, 22, 23",
            "'2023-01-01 00:00:00',31", "11, 12, 13", "21, 22, 23", "31, 32, 33"
        );

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_LIST_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00',15", "",
            "'2012-01-01 00:00:00',18",
            "(SUBPARTITION p21sp100 values in(11,21), SUBPARTITION p21sp101 values in(12,22), SUBPARTITION p21sp102 values in(13,23))",
            "'2012-01-01 00:00:00',21", "");

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_LIST_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_RANGE, SUBPART_LIST),
            "'2001-01-01 00:00:00',10", "(11,'def1'), (12,'def1'), (13,'def1')",
            "'2012-01-01 00:00:00',20", "(11,'def1'), (12,'def1'), (13,'def1')",
            "(21,'def2'), (22,'def2'), (23,'def2')",
            "'2023-01-01 00:00:00',30", "(11,'def1'), (12,'def1'), (13,'def1')",
            "(21,'def2'), (22,'def2'), (23,'def2')", "(31,'def3'), (32,'def3'), (33,'def3')"
        );

    public static final String NT_PART_RANGE_COLUMNS_SUBPART_LIST_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00',15", "",
            "'2012-01-01 00:00:00',18",
            "(SUBPARTITION p21sp100 values in((11,'def1'), (21,'def2')), SUBPARTITION p21sp101 values in((12,'def1'), (22,'def2')), SUBPARTITION p21sp102 values in((13,'def1'), (23,'def2')))",
            "'2012-01-01 00:00:00',20", "");

    // PARTITION BY LIST

    public static final String NT_PART_LIST_SUBPART_KEY =
        String.format(NT_PART_WITH_SUBPART_KEY(PART_LIST),
            "10, 11, 12",
            "20, 21, 22",
            "30, 31, 32"
        );

    public static final String NT_PART_LIST_SUBPART_KEY_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "20", "",
            "21", "(SUBPARTITION p21sp100, SUBPARTITION p21sp101, SUBPARTITION p21sp102)",
            "22", "");

    public static final String NT_PART_LIST_SUBPART_RANGE =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_RANGE),
            "10, 11, 12", "2001",
            "20, 21, 22", "2001", "2012",
            "30, 31, 32", "2001", "2012", "2023"
        );

    public static final String NT_PART_LIST_SUBPART_RANGE_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "20", "",
            "21",
            "(SUBPARTITION p21sp100 values less than(2000), SUBPARTITION p21sp101 values less than(2010), SUBPARTITION p21sp102 values less than(2012))",
            "22", "");

    public static final String NT_PART_LIST_SUBPART_RANGE_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_RANGE),
            "10, 11, 12", "10,'2011-01-01 00:00:00'",
            "20, 21, 22", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "30, 31, 32", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'", "30,'2013-01-01 00:00:00'"
        );

    public static final String NT_PART_LIST_SUBPART_RANGE_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "20", "",
            "21",
            "(SUBPARTITION p21sp100 values less than(0,'2011-01-01 00:00:00'), SUBPARTITION p21sp101 values less than(15,'2011-01-01 00:00:00'), SUBPARTITION p21sp102 values less than(20,'2011-01-01 00:00:00'))",
            "22", "");

    public static final String NT_PART_LIST_SUBPART_LIST =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_LIST),
            "10, 11, 12", "11, 12, 13",
            "20, 21, 22", "11, 12, 13", "21, 22, 23",
            "30, 31, 32", "11, 12, 13", "21, 22, 23", "31, 32, 33"
        );

    public static final String NT_PART_LIST_SUBPART_LIST_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "20", "",
            "21",
            "(SUBPARTITION p21sp100 values in (11,21), SUBPARTITION p21sp101 values in (12,22), SUBPARTITION p21sp102 values in (13,23))",
            "22", "");

    public static final String NT_PART_LIST_SUBPART_LIST_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_LIST),
            "11, 12, 13", "(11,'def1'), (12,'def1'), (13,'def1')",
            "21, 22, 23", "(11,'def1'), (12,'def1'), (13,'def1')", "(21,'def2'), (22,'def2'), (23,'def2')",
            "31, 32, 33", "(11,'def1'), (12,'def1'), (13,'def1')", "(21,'def2'), (22,'def2'), (23,'def2')",
            "(31,'def3'), (32,'def3'), (33,'def3')"
        );

    public static final String NT_PART_LIST_SUBPART_LIST_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "23", "",
            "21",
            "(SUBPARTITION p21sp100 values in ((11,'def1'), (21,'def2')), SUBPARTITION p21sp101 values in ((12,'def1'), (22,'def2')), SUBPARTITION p21sp102 values in ((13,'def1'), (23,'def2')))",
            "22", "");

    // PARTITION BY LIST COLUMNS

    public static final String NT_PART_LIST_COLUMNS_SUBPART_KEY =
        String.format(NT_PART_WITH_SUBPART_KEY(PART_LIST),
            "(10,'abc1'), (11,'abc1'), (12,'abc1')",
            "(20,'abc2'), (21,'abc2'), (22,'abc2')",
            "(30,'abc3'), (31,'abc3'), (32,'abc3')"
        );

    public static final String NT_PART_LIST_COLUMNS_SUBPART_KEY_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(20,'abc2')", "",
            "(21,'abc2')", "(SUBPARTITION p21sp100, SUBPARTITION p21sp101, SUBPARTITION p21sp102)",
            "(22,'abc2')", "");

    public static final String NT_PART_LIST_COLUMNS_SUBPART_RANGE =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_RANGE),
            "(10,'abc1'), (11,'abc1'), (12,'abc1')", "2001",
            "(20,'abc2'), (21,'abc2'), (22,'abc2')", "2001", "2012",
            "(30,'abc3'), (31,'abc3'), (32,'abc3')", "2001", "2012", "2023"
        );

    public static final String NT_PART_LIST_COLUMNS_SUBPART_RANGE_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(20,'abc2')", "",
            "(21,'abc2')",
            "(SUBPARTITION p21sp100 values less than(2000), SUBPARTITION p21sp101 values less than(2010), SUBPARTITION p21sp102 values less than(2012))",
            "(22,'abc2')", "");

    public static final String NT_PART_LIST_COLUMNS_SUBPART_RANGE_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_RANGE),
            "(10,'abc1'), (11,'abc1'), (12,'abc1')", "10,'2011-01-01 00:00:00'",
            "(20,'abc2'), (21,'abc2'), (22,'abc2')", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "(30,'abc3'), (31,'abc3'), (32,'abc3')", "10,'2011-01-01 00:00:00'", "20,'2012-01-01 00:00:00'",
            "30,'2013-01-01 00:00:00'"
        );

    public static final String NT_PART_LIST_COLUMNS_SUBPART_RANGE_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(20,'abc2')", "",
            "(21,'abc2')",
            "(SUBPARTITION p21sp100 values less than(00,'2011-01-01 00:00:00'), SUBPARTITION p21sp101 values less than(15,'2011-01-01 00:00:00'), SUBPARTITION p21sp102 values less than(20,'2011-01-01 00:00:00'))",
            "(22,'abc2')", "");

    public static final String NT_PART_LIST_COLUMNS_SUBPART_LIST =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_LIST),
            "(10,'abc1'), (11,'abc1'), (12,'abc1')", "11, 12, 13",
            "(20,'abc2'), (21,'abc2'), (22,'abc2')", "11, 12, 13", "21, 22, 23",
            "(30,'abc3'), (31,'abc3'), (32,'abc3')", "11, 12, 13", "21, 22, 23", "31, 32, 33"
        );

    public static final String NT_PART_LIST_COLUMNS_SUBPART_LIST_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(20,'abc2')", "",
            "(21,'abc2')",
            "(SUBPARTITION p21sp100 values in(11,21), SUBPARTITION p21sp101 values in(12,22), SUBPARTITION p21sp102 values in(13,23))",
            "(22,'abc2')", "");

    public static final String NT_PART_LIST_COLUMNS_SUBPART_LIST_COLUMNS =
        String.format(NT_PART_WITH_SUBPART_MIX(PART_LIST, SUBPART_LIST),
            "(11,'abc1'), (12,'abc1'), (13,'abc1')", "(11,'def1'), (12,'def1'), (13,'def1')",
            "(21,'abc2'), (22,'abc2'), (23,'abc2')", "(11,'def1'), (12,'def1'), (13,'def1')",
            "(21,'def2'), (22,'def2'), (23,'def2')",
            "(31,'abc3'), (32,'abc3'), (33,'abc3')", "(11,'def1'), (12,'def1'), (13,'def1')",
            "(21,'def2'), (22,'def2'), (23,'def2')", "(31,'def3'), (32,'def3'), (33,'def3')"
        );

    public static final String NT_PART_LIST_COLUMNS_SUBPART_LIST_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(23,'abc2')", "",
            "(21,'abc2')",
            "(SUBPARTITION p21sp100 values in((11,'def1'),(21,'def2')), SUBPARTITION p21sp101 values in((12,'def1'),(22,'def2')), SUBPARTITION p21sp102 values in((13,'def1'),(23,'def2')))",
            "(22,'abc2')", "");

    public static final String[][] PART_STRATEGIES = new String[][] {
        {PART_BY_KEY, String.format(PART_NUM, "3")},
        {PART_BY_RANGE, T_PART_RANGE},
        {PART_BY_RANGE_COLUMNS, T_PART_RANGE_COLUMNS},
        {PART_BY_LIST, T_PART_LIST},
        {PART_BY_LIST_COLUMNS, T_PART_LIST_COLUMNS}
    };

    public static final String[][] SUBPART_STRATEGIES = new String[][] {
        {SUBPART_BY_KEY, String.format(SUBPART_NUM, "3")},
        {SUBPART_BY_RANGE, T_SUBPART_RANGE},
        {SUBPART_BY_RANGE_COLUMNS, T_SUBPART_RANGE_COLUMNS},
        {SUBPART_BY_LIST, T_SUBPART_LIST},
        {SUBPART_BY_LIST_COLUMNS, T_SUBPART_LIST_COLUMNS}
    };

    public static final String T_KEY_PART_SPLIT_PART = "split partition p2";
    public static final String T_PART_RANGE_SPLIT_PART = String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
        "2008", "",
        "2010", "",
        "2012", "");

    public static final String T_PART_RANGE_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_RANGE, false), "p2",
            "'2012-01-01 00:00:00', 12", "",
            "'2012-01-01 00:00:00', 15", "",
            "'2012-01-01 00:00:00', 21", "");
    public static final String T_PART_BY_LIST_SPLIT_PART = String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
        "2", "",
        "20", "",
        "200", "");
    public static final String T_PART_BY_LIST_COLUMNS_SPLIT_PART =
        String.format(SPLIT_NOT_HASH_PART(PART_LIST, false), "p2",
            "(2,'abc2')", "",
            "(20,'abc2')", "",
            "(200,'abc2')", "");

    public static final String T_KEY_SUBPART_SPLIT_SUB_PART = "split subpartition sp2";
    public static final String T_SUBPART_RANGE_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_RANGE, true), "sp2",
            "2008", "",
            "2010", "",
            "2012", "");

    public static final String T_SUBPART_RANGE_COLUMNS_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_RANGE, true), "sp2",
            "12, '2012-01-01 00:00:00'", "",
            "15, '2012-01-01 00:00:00'", "",
            "20, '2012-01-01 00:00:00'", "");
    public static final String T_SUBPART_BY_LIST_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_LIST, true), "sp2",
            "21", "",
            "22", "",
            "23", "");

    public static final String T_SUBPART_BY_LIST_COLUMNS_SPLIT_SUB_PART =
        String.format(SPLIT_NOT_HASH_PART(SUBPART_LIST, true), "sp2",
            "(21,'def2')", "",
            "(22,'def1')", "",
            "(23,'def1')", "");

    public static final String[][] TEMPLATED_SPLIT_PART_COMBINATIONS = new String[][] {
        {PART_BY_KEY, T_KEY_PART_SPLIT_PART},
        {PART_BY_RANGE, T_PART_RANGE_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, T_PART_RANGE_COLUMNS_SPLIT_PART},
        {PART_BY_LIST, T_PART_BY_LIST_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, T_PART_BY_LIST_COLUMNS_SPLIT_PART}
    };

    public static final String[][] TEMPLATED_SPLIT_SUB_PART_COMBINATIONS = new String[][] {
        {SUBPART_BY_KEY, T_KEY_SUBPART_SPLIT_SUB_PART},
        {SUBPART_BY_RANGE, T_SUBPART_RANGE_SPLIT_SUB_PART},
        {SUBPART_BY_RANGE_COLUMNS, T_SUBPART_RANGE_COLUMNS_SPLIT_SUB_PART},
        {SUBPART_BY_LIST, T_SUBPART_BY_LIST_SPLIT_SUB_PART},
        {SUBPART_BY_LIST_COLUMNS, T_SUBPART_BY_LIST_COLUMNS_SPLIT_SUB_PART}
    };

    public static final String[][] NON_TEMPLATED_COMBINATIONS = new String[][] {
        {PART_BY_RANGE, SUBPART_BY_KEY, NT_PART_RANGE_SUBPART_KEY},
        {PART_BY_RANGE, SUBPART_BY_RANGE, NT_PART_RANGE_SUBPART_RANGE},
        {PART_BY_RANGE, SUBPART_BY_RANGE_COLUMNS, NT_PART_RANGE_SUBPART_RANGE_COLUMNS},
        {PART_BY_RANGE, SUBPART_BY_LIST, NT_PART_RANGE_SUBPART_LIST},
        {PART_BY_RANGE, SUBPART_BY_LIST_COLUMNS, NT_PART_RANGE_SUBPART_LIST_COLUMNS},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_KEY, NT_PART_RANGE_COLUMNS_SUBPART_KEY},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE, NT_PART_RANGE_COLUMNS_SUBPART_RANGE},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_PART_RANGE_COLUMNS_SUBPART_RANGE_COLUMNS},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST, NT_PART_RANGE_COLUMNS_SUBPART_LIST},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_RANGE_COLUMNS_SUBPART_LIST_COLUMNS},
        {PART_BY_LIST, SUBPART_BY_KEY, NT_PART_LIST_SUBPART_KEY},
        {PART_BY_LIST, SUBPART_BY_RANGE, NT_PART_LIST_SUBPART_RANGE},
        {PART_BY_LIST, SUBPART_BY_RANGE_COLUMNS, NT_PART_LIST_SUBPART_RANGE_COLUMNS},
        {PART_BY_LIST, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_LIST},
        {PART_BY_LIST, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_SUBPART_LIST_COLUMNS},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_KEY, NT_PART_LIST_COLUMNS_SUBPART_KEY},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE, NT_PART_LIST_COLUMNS_SUBPART_RANGE},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_RANGE_COLUMNS},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST, NT_PART_LIST_COLUMNS_SUBPART_LIST},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_LIST_COLUMNS}
    };

    public static final String[][] NON_TEMPLATED_SPLIT_PART_COMBINATIONS = new String[][] {
        {PART_BY_RANGE, SUBPART_BY_KEY, NT_PART_RANGE_SUBPART_KEY_SPLIT_PART},
        {PART_BY_RANGE, SUBPART_BY_RANGE, NT_PART_RANGE_SUBPART_RANGE_SPLIT_PART},
        {PART_BY_RANGE, SUBPART_BY_RANGE_COLUMNS, NT_PART_RANGE_SUBPART_RANGE_COLUMNS_SPLIT_PART},
        {PART_BY_RANGE, SUBPART_BY_LIST, NT_PART_RANGE_SUBPART_LIST_SPLIT_PART},
        {PART_BY_RANGE, SUBPART_BY_LIST_COLUMNS, NT_PART_RANGE_SUBPART_LIST_COLUMNS_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_KEY, NT_PART_RANGE_COLUMNS_SUBPART_KEY_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE, NT_PART_RANGE_COLUMNS_SUBPART_RANGE_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_PART_RANGE_COLUMNS_SUBPART_RANGE_COLUMNS_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST, NT_PART_RANGE_COLUMNS_SUBPART_LIST_SPLIT_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_RANGE_COLUMNS_SUBPART_LIST_COLUMNS_SPLIT_PART},
        {PART_BY_LIST, SUBPART_BY_KEY, NT_PART_LIST_SUBPART_KEY_SPLIT_PART},
        {PART_BY_LIST, SUBPART_BY_RANGE, NT_PART_LIST_SUBPART_RANGE_SPLIT_PART},
        {PART_BY_LIST, SUBPART_BY_RANGE_COLUMNS, NT_PART_LIST_SUBPART_RANGE_COLUMNS_SPLIT_PART},
        {PART_BY_LIST, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_LIST_SPLIT_PART},
        {PART_BY_LIST, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_SUBPART_LIST_COLUMNS_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_KEY, NT_PART_LIST_COLUMNS_SUBPART_KEY_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE, NT_PART_LIST_COLUMNS_SUBPART_RANGE_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_RANGE_COLUMNS_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST, NT_PART_LIST_COLUMNS_SUBPART_LIST_SPLIT_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_LIST_COLUMNS_SPLIT_PART}
    };

    public static final String[][] NON_TEMPLATED_SPLIT_SUBPART_COMBINATIONS = new String[][] {
        {PART_BY_RANGE, SUBPART_BY_KEY, NT_KEY_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE, SUBPART_BY_RANGE, NT_RANGE_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE, SUBPART_BY_RANGE_COLUMNS, NT_RANGE_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_KEY, NT_KEY_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE, NT_RANGE_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_RANGE_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_SPLIT_SUB_PART},
        {PART_BY_RANGE_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST, SUBPART_BY_KEY, NT_KEY_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST, SUBPART_BY_RANGE, NT_RANGE_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST, SUBPART_BY_RANGE_COLUMNS, NT_RANGE_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_KEY, NT_KEY_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE, NT_RANGE_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_RANGE_COLUMNS, NT_RANGE_COLUMNS_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST, NT_PART_LIST_SUBPART_SPLIT_SUB_PART},
        {PART_BY_LIST_COLUMNS, SUBPART_BY_LIST_COLUMNS, NT_PART_LIST_COLUMNS_SUBPART_SPLIT_SUB_PART}
    };

    public static final Map<String, String> getTemplatedCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(PART_STRATEGIES).forEach(p -> {
            Arrays.stream(SUBPART_STRATEGIES).forEach(sp -> {
                String key = p[0] + sp[0];
                if (p[0].contains("by key")) {
                    combinations.put(key, p[0] + " " + p[1] + "\n" + sp[0] + " " + sp[1]);
                } else {
                    combinations.put(key, p[0] + "\n" + sp[0] + "\n" + sp[1] + "\n" + p[1]);
                }
            });
        });
        return combinations;
    }

    public static final Map<String, String> getNonTemplatedCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(NON_TEMPLATED_COMBINATIONS).forEach(p -> {
            combinations.put(p[0] + p[1], p[0] + "\n" + p[1] + "\n" + p[2]);
        });
        return combinations;
    }

    public static final Map<String, String> getTemplatedSplitPartCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(TEMPLATED_SPLIT_PART_COMBINATIONS).forEach(p -> {
            combinations.put(p[0], p[1]);
        });
        return combinations;
    }

    public static final Map<String, String> getTemplatedSplitSubPartCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(TEMPLATED_SPLIT_SUB_PART_COMBINATIONS).forEach(p -> {
            combinations.put(p[0], p[1]);
        });
        return combinations;
    }

    public static final Map<String, String> getNonTemplatedSplitPartCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(NON_TEMPLATED_SPLIT_PART_COMBINATIONS).forEach(p -> {
            combinations.put(p[0] + p[1], p[2]);
        });
        return combinations;
    }

    public static final Map<String, String> getNonTemplatedSplitSubPartCombinations() {
        Map<String, String> combinations = new HashMap<>();
        Arrays.stream(NON_TEMPLATED_SPLIT_SUBPART_COMBINATIONS).forEach(p -> {
            combinations.put(p[0] + p[1], p[2]);
        });
        return combinations;
    }

    public static String T_PART_OR_SUBPART(String template) {
        return "(\n"
            + PART(template, "%s", "%s", false)
            + PART(template, "%s", "%s", false)
            + PART(template, "%s", "%s", true)
            + ")";
    }

    public static String NT_PART_WITH_SUBPART_KEY(String template) {
        return "(\n"
            + PART(template, "p1", "%s", String.format(SUBPART_NUM, "1"), false)
            + PART(template, "p2", "%s", String.format(SUBPART_NUM, "2"), false)
            + PART(template, "p3", "%s", String.format(SUBPART_NUM, "3"), true)
            + ")";
    }

    public static String NT_PART_WITH_SUBPART_MIX(String partTemplate, String subPartTemplate) {
        return "(\n"
            + PART(partTemplate, "p1", "%s", true) + "(\n"
            + PART(subPartTemplate, "p1sp1", "%s", true) + "),\n"
            + PART(partTemplate, "p2", "%s", true) + "(\n"
            + PART(subPartTemplate, "p2sp1", "%s", false)
            + PART(subPartTemplate, "p2sp2", "%s", true) + "),\n"
            + PART(partTemplate, "p3", "%s", true) + "(\n"
            + PART(subPartTemplate, "p3sp1", "%s", false)
            + PART(subPartTemplate, "p3sp2", "%s", false)
            + PART(subPartTemplate, "p3sp3", "%s", true) + ")"
            + ")";
    }

    public static String PART(String template, String rangeName, String rangeValue, boolean isLastRange) {
        return PART(template, rangeName, rangeValue, null, isLastRange);
    }

    public static String PART(String template, String rangeName, String rangeValue, String suffix,
                              boolean isLastRange) {
        String range = String.format(template, rangeName, rangeValue);
        if (TStringUtil.isNotEmpty(suffix)) {
            range += " " + suffix;
        }
        return range + (isLastRange ? "" : ",") + "\n";
    }

    public static void main(String[] args) {
        int i = 1;
        System.out.println("----------------- TEMPLATE ---------------------");
        for (String tPart : getTemplatedCombinations().values()) {
            System.out.println(tPart);
            System.out.println("-----------------" + (i++) + "---------------------");
        }
        i = 1;
        System.out.println("--------------- NON-TEMPLATE ---------------------");
        for (String ntPart : getNonTemplatedCombinations().values()) {
            System.out.println(ntPart);
            System.out.println("-----------------" + (i++) + "---------------------");
        }
    }

}
