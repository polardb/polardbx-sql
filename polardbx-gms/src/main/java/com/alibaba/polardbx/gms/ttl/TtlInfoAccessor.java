package com.alibaba.polardbx.gms.ttl;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class TtlInfoAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(TtlInfoAccessor.class);

    private static final String ALL_COLUMNS =
        "`id`,`gmt_created`,`gmt_modified`"
            + ",`table_schema`,`table_name`,`ttl_status`,`ttl_expr`,`ttl_filter`,`ttl_interval`,`ttl_unit`,`ttl_col`,`ttl_tz`,`ttl_cron`,`ttl_binlog`"
            + ",`arc_kind`,`arc_status`,`arc_tmp_tbl_schema`,`arc_tmp_tbl_name`,`arc_tbl_schema`, `arc_tbl_name`"
            + ",`arc_part_mode`,`arc_part_interval`,`arc_part_unit`,`arc_pre_part_cnt`,`arc_post_part_cnt`"
            + ",`extra`";
    private static final String ALL_VALUES = "("
        + "null,now(),now()"
        + ",?,?,?,?,?,?,?,?,?,?,?"
        + ",?,?,?,?,?,?"
        + ",?,?,?,?,?"
        + ",?"
        + ")";

    private static final String ALL_COLUMNS_SET = ""
        + "`gmt_modified`=now(),"

        + "`table_schema`=?,"
        + "`table_name`=?,"

        + "`ttl_status`=?,"
        + "`ttl_expr`=?,"
        + "`ttl_filter`=?,"
        + "`ttl_interval`=?,"
        + "`ttl_unit`=?,"
        + "`ttl_col`=?,"
        + "`ttl_tz`=?,"
        + "`ttl_cron`=?,"
        + "`ttl_binlog`=?,"

        + "`arc_kind`=?,"
        + "`arc_status`=?,"
        + "`arc_tmp_tbl_schema`=?,"
        + "`arc_tmp_tbl_name`=?,"
        + "`arc_tbl_schema`=?,"
        + "`arc_tbl_name`=?,"

        + "`arc_part_mode`=?,"
        + "`arc_part_interval`=?,"
        + "`arc_part_unit`=?,"
        + "`arc_pre_part_cnt`=?,"
        + "`arc_post_part_cnt`=?,"

        + "`extra`=?";

    private static final String INSERT_TTL_INFO =
        "insert into ttl_info (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String REPLACE_TTL_INFO =
        "replace into ttl_info (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String DELETE_TTL_INFO_BY_DB_TB = "delete from ttl_info "
        + "where table_schema=? and table_name=?";

    private static final String DELETE_TTL_INFO_BY_DB = "delete from ttl_info "
        + "where table_schema=?";

    private static final String SELECT_TTL_INFO_BY_DB_TB = "select "
        + ALL_COLUMNS
        + " from ttl_info"
        + " where table_schema=? and table_name=?";

    private static final String SELECT_TTL_INFO_BY_ARC_DB_ARC_TB = "select "
        + ALL_COLUMNS
        + " from ttl_info"
        + " where arc_tbl_schema=? and arc_tbl_name=?";

    private static final String SELECT_TTL_INFO_BY_ARC_TMP_DB_ARC_TMP_TB = "select "
        + ALL_COLUMNS
        + " from ttl_info"
        + " where arc_tmp_tbl_schema=? and arc_tmp_tbl_name=?";

    private static final String SELECT_TTL_INFO_BY_DB = "select "
        + ALL_COLUMNS
        + " from ttl_info"
        + " where table_schema=?";

    private static final String SELECT_ALL_TTL_INFO_LIST = "select "
        + ALL_COLUMNS
        + " from ttl_info"
        + " order by table_schema, table_name";

    private static final String UPDATE_ARC_TBL_TTL_INFO_BY_DB_TB =
        "update ttl_info set arc_kind=?,arc_tbl_schema=?,arc_tbl_name=?,arc_tmp_tbl_schema=?,arc_tmp_tbl_name=? where table_schema=? and table_name=?";

    private static final String UPDATE_TTL_TBL_NAME_BY_DB_TB =
        "update ttl_info set table_schema=?,table_name=? where table_schema=? and table_name=?";

    private static final String UPDATE_TTL_INFO_BY_ID = "update ttl_info set " + ALL_COLUMNS_SET + " where `id`=?";

    private static final String UPDATE_TTL_INFO_BY_DB_TB =
        "update ttl_info set " + ALL_COLUMNS_SET + " where `table_schema`=? and `table_name`=?";

    private static final String UNBIND_ARC_TBL_TTL_INFO_BY_ARC_DB_ARC_TB =
        "update ttl_info set arc_kind=0,arc_tbl_schema=null,arc_tbl_name=null,arc_tmp_tbl_schema=null,arc_tmp_tbl_name=null where arc_tbl_schema=? and arc_tbl_name=?";

    public TtlInfoRecord queryTtlInfoByDbAndTb(String tableSchema, String tableName) {

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);

            List<TtlInfoRecord> ttlInfoRecords =
                MetaDbUtil.query(SELECT_TTL_INFO_BY_DB_TB, params, TtlInfoRecord.class, connection);
            DdlMetaLogUtil.logSql(SELECT_TTL_INFO_BY_DB_TB, params);

            if (ttlInfoRecords.isEmpty()) {
                return null;
            }
            return ttlInfoRecords.get(0);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query one ttl_info", e);
        }
        return null;
    }

    public TtlInfoRecord queryTtlInfoByArcDbAndArcTb(String arcTableSchema, String arcTableName) {

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, arcTableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, arcTableName);

            List<TtlInfoRecord> ttlInfoRecords =
                MetaDbUtil.query(SELECT_TTL_INFO_BY_ARC_DB_ARC_TB, params, TtlInfoRecord.class, connection);
            DdlMetaLogUtil.logSql(SELECT_TTL_INFO_BY_ARC_DB_ARC_TB, params);

            if (ttlInfoRecords.isEmpty()) {
                return null;
            }
            return ttlInfoRecords.get(0);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query one ttl_info", e);
        }
        return null;
    }

    public TtlInfoRecord queryTtlInfoByArcTmpDbAndArcTmpTb(String arcTmpTableSchema, String arcTmpTableName) {

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, arcTmpTableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, arcTmpTableName);

            List<TtlInfoRecord> ttlInfoRecords =
                MetaDbUtil.query(SELECT_TTL_INFO_BY_ARC_TMP_DB_ARC_TMP_TB, params, TtlInfoRecord.class, connection);
            DdlMetaLogUtil.logSql(SELECT_TTL_INFO_BY_ARC_TMP_DB_ARC_TMP_TB, params);

            if (ttlInfoRecords.isEmpty()) {
                return null;
            }
            return ttlInfoRecords.get(0);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query one ttl_info by arc_tmp_tbl_schema and arc_tmp_tbl", e);
        }
        return null;
    }

    public List<TtlInfoRecord> queryTtlInfoListByDb(String tableSchema) {

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);

            List<TtlInfoRecord> ttlInfoRecords =
                MetaDbUtil.query(SELECT_TTL_INFO_BY_DB, params, TtlInfoRecord.class, connection);
            DdlMetaLogUtil.logSql(SELECT_TTL_INFO_BY_DB, params);

            return ttlInfoRecords;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query ttl_infos by db", e);
        }
        return null;
    }

    public List<TtlInfoRecord> queryAllTtlInfoList() {

        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            List<TtlInfoRecord> ttlInfoRecords =
                MetaDbUtil.query(SELECT_ALL_TTL_INFO_LIST, params, TtlInfoRecord.class, connection);
            DdlMetaLogUtil.logSql(SELECT_ALL_TTL_INFO_LIST, params);
            return ttlInfoRecords;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query all ttl_infosb", e);
        }
        return null;
    }

    public int insertNewTtlInfo(TtlInfoRecord ttlInfo) {
        int res = -1;
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();

            Integer paramIndex = 0;
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlExpr());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlFilter());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCol());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlTimezone());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCron());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlBinlog());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcKind());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartMode());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPrePartCnt());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPostPartCnt());

            JSONObject extra = new JSONObject();
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, extra.toJSONString());

            res = MetaDbUtil.insert(INSERT_TTL_INFO, params, connection);
            DdlMetaLogUtil.logSql(INSERT_TTL_INFO, params);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "add ttl_info", e);
        }

        return res;
    }

    public int replaceTtlInfo(TtlInfoRecord ttlInfo) {
        int res = -1;
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();

            Integer paramIndex = 0;
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlExpr());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlFilter());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCol());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlTimezone());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCron());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlBinlog());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcKind());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartMode());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPrePartCnt());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPostPartCnt());

            JSONObject extra = new JSONObject();
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, extra.toJSONString());

            res = MetaDbUtil.insert(REPLACE_TTL_INFO, params, connection);
            DdlMetaLogUtil.logSql(REPLACE_TTL_INFO, params);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "replace ttl_info", e);
        }

        return res;
    }

    public int updateTtlInfoByDbNameAndTbName(TtlInfoRecord ttlInfo, String ttlTblSchema, String ttlTblName) {
        int res = -1;
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();

            Integer paramIndex = 0;
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlExpr());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlFilter());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCol());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlTimezone());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCron());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlBinlog());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcKind());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartMode());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPrePartCnt());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPostPartCnt());

            JSONObject extra = new JSONObject();
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, extra.toJSONString());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblName);

            res = MetaDbUtil.insert(UPDATE_TTL_INFO_BY_DB_TB, params, connection);
            DdlMetaLogUtil.logSql(UPDATE_TTL_INFO_BY_DB_TB, params);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "update ttl_info by db.tb", e);
        }

        return res;
    }

    public int updateTtlInfoById(TtlInfoRecord ttlInfo, Long id) {
        int res = -1;
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();

            Integer paramIndex = 0;
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTableName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlExpr());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlFilter());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCol());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlTimezone());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getTtlCron());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getTtlBinlog());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcKind());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTmpTblName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlInfo.getArcTblName());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartMode());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartInterval());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPartUnit());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPrePartCnt());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, ttlInfo.getArcPostPartCnt());

            JSONObject extra = new JSONObject();
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, extra.toJSONString());

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, id);

            res = MetaDbUtil.update(UPDATE_TTL_INFO_BY_ID, params, connection);
            DdlMetaLogUtil.logSql(UPDATE_TTL_INFO_BY_ID, params);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "update ttl_info by id", e);
        }

        return res;
    }

    public int deleteTtlInfoByDbAndTb(String tableSchema, String tableName) {
        int res = -1;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);

            res = MetaDbUtil.delete(DELETE_TTL_INFO_BY_DB_TB, params, connection);
            DdlMetaLogUtil.logSql(DELETE_TTL_INFO_BY_DB_TB, params);
            return res;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "query one ttl_info by tb", e);
        }
        return res;
    }

    public int deleteByDbName(String tableSchema) {
        int res = -1;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);

            res = MetaDbUtil.delete(DELETE_TTL_INFO_BY_DB, params, connection);
            DdlMetaLogUtil.logSql(DELETE_TTL_INFO_BY_DB, params);
            return res;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "delete ttl_infos by db", e);
        }
        return res;
    }

//    public int updateArcTableInfoByByDbAndTb(String artTblSchema, String arcTblName,
//                                             String arcTmpTblSchema, String arcTmpTblName,
//                                             String ttlTblSchema, String ttlTblName) {
//        int res = -1;
//        try {
//            Map<Integer, ParameterContext> params = new HashMap<>();
//            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, artTblSchema);
//            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, arcTblName);
//            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, arcTmpTblSchema);
//            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, arcTmpTblName);
//            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, ttlTblSchema);
//            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, ttlTblName);
//
//            res = MetaDbUtil.delete(UPDATE_ARC_TBL_TTL_INFO_BY_DB_TB, params, connection);
//            DdlMetaLogUtil.logSql(UPDATE_ARC_TBL_TTL_INFO_BY_DB_TB, params);
//            return res;
//        } catch (Exception e) {
//            logAndThrow(e.getMessage(), "update ttl_infos by db and tb", e);
//        }
//        return res;
//    }

    public int updateArcTableInfoByByDbAndTb(Integer arcKind,
                                             String arcTblSchema, String arcTblName,
                                             String arcTmpTblSchema, String arcTmpTblName,
                                             String ttlTblSchema, String ttlTblName) {
        int res = -1;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            Integer paramIndex = 0;

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setInt, arcKind);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, arcTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, arcTblName);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, arcTmpTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, arcTmpTblName);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblName);

            res = MetaDbUtil.delete(UPDATE_ARC_TBL_TTL_INFO_BY_DB_TB, params, connection);
            DdlMetaLogUtil.logSql(UPDATE_ARC_TBL_TTL_INFO_BY_DB_TB, params);
            return res;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "update ttl_infos by db and tb", e);
        }
        return res;
    }

    public int updateTtlTableNameByDbAndTb(String newTblSchema, String newTblName,
                                           String ttlTblSchema, String ttlTblName) {
        int res = -1;
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            Integer paramIndex = 0;

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, newTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, newTblName);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblSchema);
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, ttlTblName);

            res = MetaDbUtil.delete(UPDATE_TTL_TBL_NAME_BY_DB_TB, params, connection);
            DdlMetaLogUtil.logSql(UPDATE_TTL_TBL_NAME_BY_DB_TB, params);
            return res;
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "update ttl tbl name by db and tb", e);
        }
        return res;
    }

    public void unBindingByArchiveTableName(String archiveTableSchema, String archiveTableName) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, archiveTableSchema);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, archiveTableName);

        try {
            DdlMetaLogUtil.logSql(UNBIND_ARC_TBL_TTL_INFO_BY_ARC_DB_ARC_TB, params);
            MetaDbUtil.update(UNBIND_ARC_TBL_TTL_INFO_BY_ARC_DB_ARC_TB, params, connection);
        } catch (Exception e) {
            logAndThrow(e.getMessage(), "unbind ttl_info by arc_db and arc_tb", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            "ttl_info", e.getMessage());
    }

}
