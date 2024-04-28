/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.exception.code;

import java.util.regex.Pattern;

public enum ErrorCode {
    // cobar error code
    ERR_OPEN_SOCKET(ErrorType.Net, 3001),
    ERR_CONNECT_SOCKET(ErrorType.Net, 3002),
    ERR_FINISH_CONNECT(ErrorType.Net, 3003),
    ERR_REGISTER(ErrorType.Net, 3004),
    ERR_READ(ErrorType.Net, 3005),
    ERR_PUT_WRITE_QUEUE(ErrorType.Net, 3006),
    ERR_WRITE_BY_EVENT(ErrorType.Net, 3007),
    ERR_WRITE_BY_QUEUE(ErrorType.Net, 3008),
    ERR_HANDLE_DATA(ErrorType.Net, 3009),
    ERR_SQL_STATE(ErrorType.Net, 3011),

    // mysql error code
    ER_HASHCHK(ErrorType.Net, 1000),
    ER_NISAMCHK(ErrorType.Net, 1001),
    ER_NO(ErrorType.Net, 1002),
    ER_YES(ErrorType.Net, 1003),
    ER_CANT_CREATE_FILE(ErrorType.Net, 1004),
    ER_CANT_CREATE_TABLE(ErrorType.Net, 1005),
    ER_CANT_CREATE_DB(ErrorType.Net, 1006),
    ER_DB_CREATE_EXISTS(ErrorType.Net, 1007),
    ER_DB_DROP_EXISTS(ErrorType.Net, 1008),
    ER_DB_DROP_DELETE(ErrorType.Net, 1009),
    ER_DB_DROP_RMDIR(ErrorType.Net, 1010),
    ER_CANT_DELETE_FILE(ErrorType.Net, 1011),
    ER_CANT_FIND_SYSTEM_REC(ErrorType.Net, 1012),
    ER_CANT_GET_STAT(ErrorType.Net, 1013),
    ER_CANT_GET_WD(ErrorType.Net, 1014),
    ER_CANT_LOCK(ErrorType.Net, 1015),
    ER_CANT_OPEN_FILE(ErrorType.Net, 1016),
    ER_FILE_NOT_FOUND(ErrorType.Net, 1017),
    ER_CANT_READ_DIR(ErrorType.Net, 1018),
    ER_CANT_SET_WD(ErrorType.Net, 1019),
    ER_CHECKREAD(ErrorType.Net, 1020),
    ER_DISK_FULL(ErrorType.Net, 1021),
    ER_DUP_KEY(ErrorType.Net, 1022),
    ER_ERROR_ON_CLOSE(ErrorType.Net, 1023),
    ER_ERROR_ON_READ(ErrorType.Net, 1024),
    ER_ERROR_ON_RENAME(ErrorType.Net, 1025),
    ER_ERROR_ON_WRITE(ErrorType.Net, 1026),
    ER_FILE_USED(ErrorType.Net, 1027),
    ER_FILSORT_ABORT(ErrorType.Net, 1028),
    ER_FORM_NOT_FOUND(ErrorType.Net, 1029),
    ER_GET_ERRNO(ErrorType.Net, 1030),
    ER_ILLEGAL_HA(ErrorType.Net, 1031),
    ER_KEY_NOT_FOUND(ErrorType.Net, 1032),
    ER_NOT_FORM_FILE(ErrorType.Net, 1033),
    ER_NOT_KEYFILE(ErrorType.Net, 1034),
    ER_OLD_KEYFILE(ErrorType.Net, 1035),
    ER_OPEN_AS_READONLY(ErrorType.Net, 1036),
    ER_OUTOFMEMORY(ErrorType.Net, 1037),
    ER_OUT_OF_SORTMEMORY(ErrorType.Net, 1038),
    ER_UNEXPECTED_EOF(ErrorType.Net, 1039),
    ER_CON_COUNT_ERROR(ErrorType.Net, 1040),
    ER_OUT_OF_RESOURCES(ErrorType.Net, 1041),
    ER_BAD_HOST_ERROR(ErrorType.Net, 1042),
    ER_HANDSHAKE_ERROR(ErrorType.Net, 1043),
    ER_DBACCESS_DENIED_ERROR(ErrorType.Net, 1044),
    ER_ACCESS_DENIED_ERROR(ErrorType.Net, 1045),
    ER_NO_DB_ERROR(ErrorType.Net, 1046),
    ER_UNKNOWN_COM_ERROR(ErrorType.Net, 1047),
    ER_BAD_NULL_ERROR(ErrorType.Net, 1048),
    ER_BAD_DB_ERROR(ErrorType.Net, 1049),
    ER_TABLE_EXISTS_ERROR(ErrorType.Net, 1050),
    ER_BAD_TABLE_ERROR(ErrorType.Net, 1051),
    ER_NON_UNIQ_ERROR(ErrorType.Net, 1052),
    ER_SERVER_SHUTDOWN(ErrorType.Net, 1053),
    ER_BAD_FIELD_ERROR(ErrorType.Net, 1054),
    ER_WRONG_FIELD_WITH_GROUP(ErrorType.Net, 1055),
    ER_WRONG_GROUP_FIELD(ErrorType.Net, 1056),
    ER_WRONG_SUM_SELECT(ErrorType.Net, 1057),
    ER_WRONG_VALUE_COUNT(ErrorType.Net, 1058),
    ER_TOO_LONG_IDENT(ErrorType.Net, 1059),
    ER_DUP_FIELDNAME(ErrorType.Net, 1060),
    ER_DUP_KEYNAME(ErrorType.Net, 1061),
    ER_DUP_ENTRY(ErrorType.Net, 1062),
    ER_WRONG_FIELD_SPEC(ErrorType.Net, 1063),
    ER_PARSE_ERROR(ErrorType.Net, 1064),
    ER_EMPTY_QUERY(ErrorType.Net, 1065),
    ER_NONUNIQ_TABLE(ErrorType.Net, 1066),
    ER_INVALID_DEFAULT(ErrorType.Net, 1067),
    ER_MULTIPLE_PRI_KEY(ErrorType.Net, 1068),
    ER_TOO_MANY_KEYS(ErrorType.Net, 1069),
    ER_TOO_MANY_KEY_PARTS(ErrorType.Net, 1070),
    ER_TOO_LONG_KEY(ErrorType.Net, 1071),
    ER_KEY_COLUMN_DOES_NOT_EXITS(ErrorType.Net, 1072),
    ER_BLOB_USED_AS_KEY(ErrorType.Net, 1073),
    ER_TOO_BIG_FIELDLENGTH(ErrorType.Net, 1074),
    ER_WRONG_AUTO_KEY(ErrorType.Net, 1075),
    ER_READY(ErrorType.Net, 1076),
    ER_NORMAL_SHUTDOWN(ErrorType.Net, 1077),
    ER_GOT_SIGNAL(ErrorType.Net, 1078),
    ER_SHUTDOWN_COMPLETE(ErrorType.Net, 1079),
    ER_FORCING_CLOSE(ErrorType.Net, 1080),
    ER_IPSOCK_ERROR(ErrorType.Net, 1081),
    ER_NO_SUCH_INDEX(ErrorType.Net, 1082),
    ER_WRONG_FIELD_TERMINATORS(ErrorType.Net, 1083),
    ER_BLOBS_AND_NO_TERMINATED(ErrorType.Net, 1084),
    ER_TEXTFILE_NOT_READABLE(ErrorType.Net, 1085),
    ER_FILE_EXISTS_ERROR(ErrorType.Net, 1086),
    ER_LOAD_INFO(ErrorType.Net, 1087),
    ER_ALTER_INFO(ErrorType.Net, 1088),
    ER_WRONG_SUB_KEY(ErrorType.Net, 1089),
    ER_CANT_REMOVE_ALL_FIELDS(ErrorType.Net, 1090),
    ER_CANT_DROP_FIELD_OR_KEY(ErrorType.Net, 1091),
    ER_INSERT_INFO(ErrorType.Net, 1092),
    ER_UPDATE_TABLE_USED(ErrorType.Net, 1093),
    ER_NO_SUCH_THREAD(ErrorType.Net, 1094),
    ER_KILL_DENIED_ERROR(ErrorType.Net, 1095),
    ER_NO_TABLES_USED(ErrorType.Net, 1096),
    ER_TOO_BIG_SET(ErrorType.Net, 1097),
    ER_NO_UNIQUE_LOGFILE(ErrorType.Net, 1098),
    ER_TABLE_NOT_LOCKED_FOR_WRITE(ErrorType.Net, 1099),
    ER_TABLE_NOT_LOCKED(ErrorType.Net, 1100),
    ER_BLOB_CANT_HAVE_DEFAULT(ErrorType.Net, 1101),
    ER_WRONG_DB_NAME(ErrorType.Net, 1102),
    ER_WRONG_TABLE_NAME(ErrorType.Net, 1103),
    ER_TOO_BIG_SELECT(ErrorType.Net, 1104),
    ER_UNKNOWN_ERROR(ErrorType.Net, 1105),
    ER_UNKNOWN_PROCEDURE(ErrorType.Net, 1106),
    ER_WRONG_PARAMCOUNT_TO_PROCEDURE(ErrorType.Net, 1107),
    ER_WRONG_PARAMETERS_TO_PROCEDURE(ErrorType.Net, 1108),
    ER_UNKNOWN_TABLE(ErrorType.Net, 1109),
    ER_FIELD_SPECIFIED_TWICE(ErrorType.Net, 1110),
    ER_INVALID_GROUP_FUNC_USE(ErrorType.Net, 1111),
    ER_UNSUPPORTED_EXTENSION(ErrorType.Net, 1112),
    ER_TABLE_MUST_HAVE_COLUMNS(ErrorType.Net, 1113),
    ER_RECORD_FILE_FULL(ErrorType.Net, 1114),
    ER_UNKNOWN_CHARACTER_SET(ErrorType.Net, 1115),
    ER_TOO_MANY_TABLES(ErrorType.Net, 1116),
    ER_TOO_MANY_FIELDS(ErrorType.Net, 1117),
    ER_TOO_BIG_ROWSIZE(ErrorType.Net, 1118),
    ER_STACK_OVERRUN(ErrorType.Net, 1119),
    ER_WRONG_OUTER_JOIN(ErrorType.Net, 1120),
    ER_NULL_COLUMN_IN_INDEX(ErrorType.Net, 1121),
    ER_CANT_FIND_UDF(ErrorType.Net, 1122),
    ER_CANT_INITIALIZE_UDF(ErrorType.Net, 1123),
    ER_UDF_NO_PATHS(ErrorType.Net, 1124),
    ER_UDF_EXISTS(ErrorType.Net, 1125),
    ER_CANT_OPEN_LIBRARY(ErrorType.Net, 1126),
    ER_CANT_FIND_DL_ENTRY(ErrorType.Net, 1127),
    ER_FUNCTION_NOT_DEFINED(ErrorType.Net, 1128),
    ER_HOST_IS_BLOCKED(ErrorType.Net, 1129),
    ER_HOST_NOT_PRIVILEGED(ErrorType.Net, 1130),
    ER_PASSWORD_ANONYMOUS_USER(ErrorType.Net, 1131),
    ER_PASSWORD_NOT_ALLOWED(ErrorType.Net, 1132),
    ER_PASSWORD_NO_MATCH(ErrorType.Net, 1133),
    ER_UPDATE_INFO(ErrorType.Net, 1134),
    ER_CANT_CREATE_THREAD(ErrorType.Net, 1135),
    ER_WRONG_VALUE_COUNT_ON_ROW(ErrorType.Net, 1136),
    ER_CANT_REOPEN_TABLE(ErrorType.Net, 1137),
    ER_INVALID_USE_OF_NULL(ErrorType.Net, 1138),
    ER_REGEXP_ERROR(ErrorType.Net, 1139),
    ER_MIX_OF_GROUP_FUNC_AND_FIELDS(ErrorType.Net, 1140),
    ER_NONEXISTING_GRANT(ErrorType.Net, 1141),
    ER_TABLEACCESS_DENIED_ERROR(ErrorType.Net, 1142),
    ER_COLUMNACCESS_DENIED_ERROR(ErrorType.Net, 1143),
    ER_ILLEGAL_GRANT_FOR_TABLE(ErrorType.Net, 1144),
    ER_GRANT_WRONG_HOST_OR_USER(ErrorType.Net, 1145),
    ER_NO_SUCH_TABLE(ErrorType.Net, 1146),
    ER_NONEXISTING_TABLE_GRANT(ErrorType.Net, 1147),
    ER_NOT_ALLOWED_COMMAND(ErrorType.Net, 1148),
    ER_SYNTAX_ERROR(ErrorType.Net, 1149),
    ER_DELAYED_CANT_CHANGE_LOCK(ErrorType.Net, 1150),
    ER_TOO_MANY_DELAYED_THREADS(ErrorType.Net, 1151),
    ER_ABORTING_CONNECTION(ErrorType.Net, 1152),
    ER_NET_PACKET_TOO_LARGE(ErrorType.Net, 1153),
    ER_NET_READ_ERROR_FROM_PIPE(ErrorType.Net, 1154),
    ER_NET_FCNTL_ERROR(ErrorType.Net, 1155),
    ER_NET_PACKETS_OUT_OF_ORDER(ErrorType.Net, 1156),
    ER_NET_UNCOMPRESS_ERROR(ErrorType.Net, 1157),
    ER_NET_READ_ERROR(ErrorType.Net, 1158),
    ER_NET_READ_INTERRUPTED(ErrorType.Net, 1159),
    ER_NET_ERROR_ON_WRITE(ErrorType.Net, 1160),
    ER_NET_WRITE_INTERRUPTED(ErrorType.Net, 1161),
    ER_TOO_LONG_STRING(ErrorType.Net, 1162),
    ER_TABLE_CANT_HANDLE_BLOB(ErrorType.Net, 1163),
    ER_TABLE_CANT_HANDLE_AUTO_INCREMENT(ErrorType.Net, 1164),
    ER_DELAYED_INSERT_TABLE_LOCKED(ErrorType.Net, 1165),
    ER_WRONG_COLUMN_NAME(ErrorType.Net, 1166),
    ER_WRONG_KEY_COLUMN(ErrorType.Net, 1167),
    ER_WRONG_MRG_TABLE(ErrorType.Net, 1168),
    ER_DUP_UNIQUE(ErrorType.Net, 1169),
    ER_BLOB_KEY_WITHOUT_LENGTH(ErrorType.Net, 1170),
    ER_PRIMARY_CANT_HAVE_NULL(ErrorType.Net, 1171),
    ER_TOO_MANY_ROWS(ErrorType.Net, 1172),
    ER_REQUIRES_PRIMARY_KEY(ErrorType.Net, 1173),
    ER_NO_RAID_COMPILED(ErrorType.Net, 1174),
    ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE(ErrorType.Net, 1175),
    ER_KEY_DOES_NOT_EXITS(ErrorType.Net, 1176),
    ER_CHECK_NO_SUCH_TABLE(ErrorType.Net, 1177),
    ER_CHECK_NOT_IMPLEMENTED(ErrorType.Net, 1178),
    ER_CANT_DO_THIS_DURING_AN_TRANSACTION(ErrorType.Net, 1179),
    ER_ERROR_DURING_COMMIT(ErrorType.Net, 1180),
    ER_ERROR_DURING_ROLLBACK(ErrorType.Net, 1181),
    ER_ERROR_DURING_FLUSH_LOGS(ErrorType.Net, 1182),
    ER_ERROR_DURING_CHECKPO(ErrorType.Net, 1183),
    ER_NEW_ABORTING_CONNECTION(ErrorType.Net, 1184),
    ER_DUMP_NOT_IMPLEMENTED(ErrorType.Net, 1185),
    ER_FLUSH_MASTER_BINLOG_CLOSED(ErrorType.Net, 1186),
    ER_INDEX_REBUILD(ErrorType.Net, 1187),
    ER_MASTER(ErrorType.Net, 1188),
    ER_MASTER_NET_READ(ErrorType.Net, 1189),
    ER_MASTER_NET_WRITE(ErrorType.Net, 1190),
    ER_FT_MATCHING_KEY_NOT_FOUND(ErrorType.Net, 1191),
    ER_LOCK_OR_ACTIVE_TRANSACTION(ErrorType.Net, 1192),
    ER_UNKNOWN_SYSTEM_VARIABLE(ErrorType.Net, 1193),
    ER_CRASHED_ON_USAGE(ErrorType.Net, 1194),
    ER_CRASHED_ON_REPAIR(ErrorType.Net, 1195),
    ER_WARNING_NOT_COMPLETE_ROLLBACK(ErrorType.Net, 1196),
    ER_TRANS_CACHE_FULL(ErrorType.Net, 1197),
    ER_SLAVE_MUST_STOP(ErrorType.Net, 1198),
    ER_SLAVE_NOT_RUNNING(ErrorType.Net, 1199),
    ER_BAD_SLAVE(ErrorType.Net, 1200),
    ER_MASTER_INFO(ErrorType.Net, 1201),
    ER_SLAVE_THREAD(ErrorType.Net, 1202),
    ER_TOO_MANY_USER_CONNECTIONS(ErrorType.Net, 1203),
    ER_SET_CONSTANTS_ONLY(ErrorType.Net, 1204),
    ER_LOCK_WAIT_TIMEOUT(ErrorType.Net, 1205),
    ER_LOCK_TABLE_FULL(ErrorType.Net, 1206),
    ER_READ_ONLY_TRANSACTION(ErrorType.Net, 1207),
    ER_DROP_DB_WITH_READ_LOCK(ErrorType.Net, 1208),
    ER_CREATE_DB_WITH_READ_LOCK(ErrorType.Net, 1209),
    ER_WRONG_ARGUMENTS(ErrorType.Net, 1210),
    ER_NO_PERMISSION_TO_CREATE_USER(ErrorType.Net, 1211),
    ER_UNION_TABLES_IN_DIFFERENT_DIR(ErrorType.Net, 1212),
    ER_LOCK_DEADLOCK(ErrorType.Net, 1213),
    ER_TABLE_CANT_HANDLE_FT(ErrorType.Net, 1214),
    ER_CANNOT_ADD_FOREIGN(ErrorType.Net, 1215),
    ER_NO_REFERENCED_ROW(ErrorType.Net, 1216),
    ER_ROW_IS_REFERENCED(ErrorType.Net, 1217),
    ER_CONNECT_TO_MASTER(ErrorType.Net, 1218),
    ER_QUERY_ON_MASTER(ErrorType.Net, 1219),
    ER_ERROR_WHEN_EXECUTING_COMMAND(ErrorType.Net, 1220),
    ER_WRONG_USAGE(ErrorType.Net, 1221),
    ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT(ErrorType.Net, 1222),
    ER_CANT_UPDATE_WITH_READLOCK(ErrorType.Net, 1223),
    ER_MIXING_NOT_ALLOWED(ErrorType.Net, 1224),
    ER_DUP_ARGUMENT(ErrorType.Net, 1225),
    ER_USER_LIMIT_REACHED(ErrorType.Net, 1226),
    ER_SPECIFIC_ACCESS_DENIED_ERROR(ErrorType.Net, 1227),
    ER_LOCAL_VARIABLE(ErrorType.Net, 1228),
    ER_GLOBAL_VARIABLE(ErrorType.Net, 1229),
    ER_NO_DEFAULT(ErrorType.Net, 1230),
    ER_WRONG_VALUE_FOR_VAR(ErrorType.Net, 1231),
    ER_WRONG_TYPE_FOR_VAR(ErrorType.Net, 1232),
    ER_VAR_CANT_BE_READ(ErrorType.Net, 1233),
    ER_CANT_USE_OPTION_HERE(ErrorType.Net, 1234),
    ER_NOT_SUPPORTED_YET(ErrorType.Net, 1235),
    ER_MASTER_FATAL_ERROR_READING_BINLOG(ErrorType.Net, 1236),
    ER_SLAVE_IGNORED_TABLE(ErrorType.Net, 1237),
    ER_INCORRECT_GLOBAL_LOCAL_VAR(ErrorType.Net, 1238),
    ER_WRONG_FK_DEF(ErrorType.Net, 1239),
    ER_KEY_REF_DO_NOT_MATCH_TABLE_REF(ErrorType.Net, 1240),
    ER_OPERAND_COLUMNS(ErrorType.Net, 1241),
    ER_SUBQUERY_NO_1_ROW(ErrorType.Net, 1242),
    ER_UNKNOWN_STMT_HANDLER(ErrorType.Net, 1243),
    ER_CORRUPT_HELP_DB(ErrorType.Net, 1244),
    ER_CYCLIC_REFERENCE(ErrorType.Net, 1245),
    ER_AUTO_CONVERT(ErrorType.Net, 1246),
    ER_ILLEGAL_REFERENCE(ErrorType.Net, 1247),
    ER_DERIVED_MUST_HAVE_ALIAS(ErrorType.Net, 1248),
    ER_SELECT_REDUCED(ErrorType.Net, 1249),
    ER_TABLENAME_NOT_ALLOWED_HERE(ErrorType.Net, 1250),
    ER_NOT_SUPPORTED_AUTH_MODE(ErrorType.Net, 1251),
    ER_SPATIAL_CANT_HAVE_NULL(ErrorType.Net, 1252),
    ER_COLLATION_CHARSET_MISMATCH(ErrorType.Net, 1253),
    ER_SLAVE_WAS_RUNNING(ErrorType.Net, 1254),
    ER_SLAVE_WAS_NOT_RUNNING(ErrorType.Net, 1255),
    ER_TOO_BIG_FOR_UNCOMPRESS(ErrorType.Net, 1256),
    ER_ZLIB_Z_MEM_ERROR(ErrorType.Net, 1257),
    ER_ZLIB_Z_BUF_ERROR(ErrorType.Net, 1258),
    ER_ZLIB_Z_DATA_ERROR(ErrorType.Net, 1259),
    ER_CUT_VALUE_GROUP_CONCAT(ErrorType.Net, 1260),
    ER_WARN_TOO_FEW_RECORDS(ErrorType.Net, 1261),
    ER_WARN_TOO_MANY_RECORDS(ErrorType.Net, 1262),
    ER_WARN_NULL_TO_NOTNULL(ErrorType.Net, 1263),
    ER_WARN_DATA_OUT_OF_RANGE(ErrorType.Net, 1264),
    WARN_DATA_TRUNCATED(ErrorType.Net, 1265),
    ER_WARN_USING_OTHER_HANDLER(ErrorType.Net, 1266),
    ER_CANT_AGGREGATE_2COLLATIONS(ErrorType.Net, 1267),
    ER_DROP_USER(ErrorType.Net, 1268),
    ER_REVOKE_GRANTS(ErrorType.Net, 1269),
    ER_CANT_AGGREGATE_3COLLATIONS(ErrorType.Net, 1270),
    ER_CANT_AGGREGATE_NCOLLATIONS(ErrorType.Net, 1271),
    ER_VARIABLE_IS_NOT_STRUCT(ErrorType.Net, 1272),
    ER_UNKNOWN_COLLATION(ErrorType.Net, 1273),
    ER_SLAVE_IGNORED_SSL_PARAMS(ErrorType.Net, 1274),
    ER_SERVER_IS_IN_SECURE_AUTH_MODE(ErrorType.Net, 1275),
    ER_WARN_FIELD_RESOLVED(ErrorType.Net, 1276),
    ER_BAD_SLAVE_UNTIL_COND(ErrorType.Net, 1277),
    ER_MISSING_SKIP_SLAVE(ErrorType.Net, 1278),
    ER_UNTIL_COND_IGNORED(ErrorType.Net, 1279),
    ER_WRONG_NAME_FOR_INDEX(ErrorType.Net, 1280),
    ER_WRONG_NAME_FOR_CATALOG(ErrorType.Net, 1281),
    ER_WARN_QC_RESIZE(ErrorType.Net, 1282),
    ER_BAD_FT_COLUMN(ErrorType.Net, 1283),
    ER_UNKNOWN_KEY_CACHE(ErrorType.Net, 1284),
    ER_WARN_HOSTNAME_WONT_WORK(ErrorType.Net, 1285),
    ER_UNKNOWN_STORAGE_ENGINE(ErrorType.Net, 1286),
    ER_WARN_DEPRECATED_SYNTAX(ErrorType.Net, 1287),
    ER_NON_UPDATABLE_TABLE(ErrorType.Net, 1288),
    ER_FEATURE_DISABLED(ErrorType.Net, 1289),
    ER_OPTION_PREVENTS_STATEMENT(ErrorType.Net, 1290),
    ER_DUPLICATED_VALUE_IN_TYPE(ErrorType.Net, 1291),
    ER_TRUNCATED_WRONG_VALUE(ErrorType.Net, 1292),
    ER_TOO_MUCH_AUTO_TIMESTAMP_COLS(ErrorType.Net, 1293),
    ER_INVALID_ON_UPDATE(ErrorType.Net, 1294),
    ER_UNSUPPORTED_PS(ErrorType.Net, 1295),
    ER_GET_ERRMSG(ErrorType.Net, 1296),
    ER_GET_TEMPORARY_ERRMSG(ErrorType.Net, 1297),
    ER_UNKNOWN_TIME_ZONE(ErrorType.Net, 1298),
    ER_WARN_INVALID_TIMESTAMP(ErrorType.Net, 1299),
    ER_INVALID_CHARACTER_STRING(ErrorType.Net, 1300),
    ER_WARN_ALLOWED_PACKET_OVERFLOWED(ErrorType.Net, 1301),
    ER_CONFLICTING_DECLARATIONS(ErrorType.Net, 1302),
    ER_SP_NO_RECURSIVE_CREATE(ErrorType.Net, 1303),
    ER_SP_ALREADY_EXISTS(ErrorType.Net, 1304),
    ER_SP_DOES_NOT_EXIST(ErrorType.Net, 1305),
    ER_SP_DROP_FAILED(ErrorType.Net, 1306),
    ER_SP_STORE_FAILED(ErrorType.Net, 1307),
    ER_SP_LILABEL_MISMATCH(ErrorType.Net, 1308),
    ER_SP_LABEL_REDEFINE(ErrorType.Net, 1309),
    ER_SP_LABEL_MISMATCH(ErrorType.Net, 1310),
    ER_SP_UNINIT_VAR(ErrorType.Net, 1311),
    ER_SP_BADSELECT(ErrorType.Net, 1312),
    ER_SP_BADRETURN(ErrorType.Net, 1313),
    ER_SP_BADSTATEMENT(ErrorType.Net, 1314),
    ER_UPDATE_LOG_DEPRECATED_IGNORED(ErrorType.Net, 1315),
    ER_UPDATE_LOG_DEPRECATED_TRANSLATED(ErrorType.Net, 1316),
    ER_QUERY_INTERRUPTED(ErrorType.Net, 1317),
    ER_SP_WRONG_NO_OF_ARGS(ErrorType.Net, 1318),
    ER_SP_COND_MISMATCH(ErrorType.Net, 1319),
    ER_SP_NORETURN(ErrorType.Net, 1320),
    ER_SP_NORETURNEND(ErrorType.Net, 1321),
    ER_SP_BAD_CURSOR_QUERY(ErrorType.Net, 1322),
    ER_SP_BAD_CURSOR_SELECT(ErrorType.Net, 1323),
    ER_SP_CURSOR_MISMATCH(ErrorType.Net, 1324),
    ER_SP_CURSOR_ALREADY_OPEN(ErrorType.Net, 1325),
    ER_SP_CURSOR_NOT_OPEN(ErrorType.Net, 1326),
    ER_SP_UNDECLARED_VAR(ErrorType.Net, 1327),
    ER_SP_WRONG_NO_OF_FETCH_ARGS(ErrorType.Net, 1328),
    ER_SP_FETCH_NO_DATA(ErrorType.Net, 1329),
    ER_SP_DUP_PARAM(ErrorType.Net, 1330),
    ER_SP_DUP_VAR(ErrorType.Net, 1331),
    ER_SP_DUP_COND(ErrorType.Net, 1332),
    ER_SP_DUP_CURS(ErrorType.Net, 1333),
    ER_SP_CANT_ALTER(ErrorType.Net, 1334),
    ER_SP_SUBSELECT_NYI(ErrorType.Net, 1335),
    ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG(ErrorType.Net, 1336),
    ER_SP_VARCOND_AFTER_CURSHNDLR(ErrorType.Net, 1337),
    ER_SP_CURSOR_AFTER_HANDLER(ErrorType.Net, 1338),
    ER_SP_CASE_NOT_FOUND(ErrorType.Net, 1339),
    ER_FPARSER_TOO_BIG_FILE(ErrorType.Net, 1340),
    ER_FPARSER_BAD_HEADER(ErrorType.Net, 1341),
    ER_FPARSER_EOF_IN_COMMENT(ErrorType.Net, 1342),
    ER_FPARSER_ERROR_IN_PARAMETER(ErrorType.Net, 1343),
    ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER(ErrorType.Net, 1344),
    ER_VIEW_NO_EXPLAIN(ErrorType.Net, 1345),
    ER_FRM_UNKNOWN_TYPE(ErrorType.Net, 1346),
    ER_WRONG_OBJECT(ErrorType.Net, 1347),
    ER_NONUPDATEABLE_COLUMN(ErrorType.Net, 1348),
    ER_VIEW_SELECT_DERIVED(ErrorType.Net, 1349),
    ER_VIEW_SELECT_CLAUSE(ErrorType.Net, 1350),
    ER_VIEW_SELECT_VARIABLE(ErrorType.Net, 1351),
    ER_VIEW_SELECT_TMPTABLE(ErrorType.Net, 1352),
    ER_VIEW_WRONG_LIST(ErrorType.Net, 1353),
    ER_WARN_VIEW_MERGE(ErrorType.Net, 1354),
    ER_WARN_VIEW_WITHOUT_KEY(ErrorType.Net, 1355),
    ER_VIEW_INVALID(ErrorType.Net, 1356),
    ER_SP_NO_DROP_SP(ErrorType.Net, 1357),
    ER_SP_GOTO_IN_HNDLR(ErrorType.Net, 1358),
    ER_TRG_ALREADY_EXISTS(ErrorType.Net, 1359),
    ER_TRG_DOES_NOT_EXIST(ErrorType.Net, 1360),
    ER_TRG_ON_VIEW_OR_TEMP_TABLE(ErrorType.Net, 1361),
    ER_TRG_CANT_CHANGE_ROW(ErrorType.Net, 1362),
    ER_TRG_NO_SUCH_ROW_IN_TRG(ErrorType.Net, 1363),
    ER_NO_DEFAULT_FOR_FIELD(ErrorType.Net, 1364),
    ER_DIVISION_BY_ZERO(ErrorType.Net, 1365),
    ER_TRUNCATED_WRONG_VALUE_FOR_FIELD(ErrorType.Net, 1366),
    ER_ILLEGAL_VALUE_FOR_TYPE(ErrorType.Net, 1367),
    ER_VIEW_NONUPD_CHECK(ErrorType.Net, 1368),
    ER_VIEW_CHECK_FAILED(ErrorType.Net, 1369),
    ER_PROCACCESS_DENIED_ERROR(ErrorType.Net, 1370),
    ER_RELAY_LOG_FAIL(ErrorType.Net, 1371),
    ER_PASSWD_LENGTH(ErrorType.Net, 1372),
    ER_UNKNOWN_TARGET_BINLOG(ErrorType.Net, 1373),
    ER_IO_ERR_LOG_INDEX_READ(ErrorType.Net, 1374),
    ER_BINLOG_PURGE_PROHIBITED(ErrorType.Net, 1375),
    ER_FSEEK_FAIL(ErrorType.Net, 1376),
    ER_BINLOG_PURGE_FATAL_ERR(ErrorType.Net, 1377),
    ER_LOG_IN_USE(ErrorType.Net, 1378),
    ER_LOG_PURGE_UNKNOWN_ERR(ErrorType.Net, 1379),
    ER_RELAY_LOG_INIT(ErrorType.Net, 1380),
    ER_NO_BINARY_LOGGING(ErrorType.Net, 1381),
    ER_RESERVED_SYNTAX(ErrorType.Net, 1382),
    ER_WSAS_FAILED(ErrorType.Net, 1383),
    ER_DIFF_GROUPS_PROC(ErrorType.Net, 1384),
    ER_NO_GROUP_FOR_PROC(ErrorType.Net, 1385),
    ER_ORDER_WITH_PROC(ErrorType.Net, 1386),
    ER_LOGGING_PROHIBIT_CHANGING_OF(ErrorType.Net, 1387),
    ER_NO_FILE_MAPPING(ErrorType.Net, 1388),
    ER_WRONG_MAGIC(ErrorType.Net, 1389),
    ER_PS_MANY_PARAM(ErrorType.Net, 1390),
    ER_KEY_PART_0(ErrorType.Net, 1391),
    ER_VIEW_CHECKSUM(ErrorType.Net, 1392),
    ER_VIEW_MULTIUPDATE(ErrorType.Net, 1393),
    ER_VIEW_NO_INSERT_FIELD_LIST(ErrorType.Net, 1394),
    ER_VIEW_DELETE_MERGE_VIEW(ErrorType.Net, 1395),
    ER_CANNOT_USER(ErrorType.Net, 1396),
    ER_XAER_NOTA(ErrorType.Net, 1397),
    ER_XAER_INVAL(ErrorType.Net, 1398),
    ER_XAER_RMFAIL(ErrorType.Net, 1399),
    ER_XAER_OUTSIDE(ErrorType.Net, 1400),
    ER_XAER_RMERR(ErrorType.Net, 1401),
    ER_XA_RBROLLBACK(ErrorType.Net, 1402),
    ER_NONEXISTING_PROC_GRANT(ErrorType.Net, 1403),
    ER_PROC_AUTO_GRANT_FAIL(ErrorType.Net, 1404),
    ER_PROC_AUTO_REVOKE_FAIL(ErrorType.Net, 1405),
    ER_DATA_TOO_LONG(ErrorType.Net, 1406),
    ER_SP_BAD_SQLSTATE(ErrorType.Net, 1407),
    ER_STARTUP(ErrorType.Net, 1408),
    ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR(ErrorType.Net, 1409),
    ER_CANT_CREATE_USER_WITH_GRANT(ErrorType.Net, 1410),
    ER_WRONG_VALUE_FOR_TYPE(ErrorType.Net, 1411),
    ER_TABLE_DEF_CHANGED(ErrorType.Net, 1412),
    ER_SP_DUP_HANDLER(ErrorType.Net, 1413),
    ER_SP_NOT_VAR_ARG(ErrorType.Net, 1414),
    ER_SP_NO_RETSET(ErrorType.Net, 1415),
    ER_CANT_CREATE_GEOMETRY_OBJECT(ErrorType.Net, 1416),
    ER_FAILED_ROUTINE_BREAK_BINLOG(ErrorType.Net, 1417),
    ER_BINLOG_UNSAFE_ROUTINE(ErrorType.Net, 1418),
    ER_BINLOG_CREATE_ROUTINE_NEED_SUPER(ErrorType.Net, 1419),
    ER_EXEC_STMT_WITH_OPEN_CURSOR(ErrorType.Net, 1420),
    ER_STMT_HAS_NO_OPEN_CURSOR(ErrorType.Net, 1421),
    ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG(ErrorType.Net, 1422),
    ER_NO_DEFAULT_FOR_VIEW_FIELD(ErrorType.Net, 1423),
    ER_SP_NO_RECURSION(ErrorType.Net, 1424),
    ER_TOO_BIG_SCALE(ErrorType.Net, 1425),
    ER_TOO_BIG_PRECISION(ErrorType.Net, 1426),
    ER_M_BIGGER_THAN_D(ErrorType.Net, 1427),
    ER_WRONG_LOCK_OF_SYSTEM_TABLE(ErrorType.Net, 1428),
    ER_CONNECT_TO_FOREIGN_DATA_SOURCE(ErrorType.Net, 1429),
    ER_QUERY_ON_FOREIGN_DATA_SOURCE(ErrorType.Net, 1430),
    ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST(ErrorType.Net, 1431),
    ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE(ErrorType.Net, 1432),
    ER_FOREIGN_DATA_STRING_INVALID(ErrorType.Net, 1433),
    ER_CANT_CREATE_FEDERATED_TABLE(ErrorType.Net, 1434),
    ER_TRG_IN_WRONG_SCHEMA(ErrorType.Net, 1435),
    ER_STACK_OVERRUN_NEED_MORE(ErrorType.Net, 1436),
    ER_TOO_LONG_BODY(ErrorType.Net, 1437),
    ER_WARN_CANT_DROP_DEFAULT_KEYCACHE(ErrorType.Net, 1438),
    ER_TOO_BIG_DISPLAYWIDTH(ErrorType.Net, 1439),
    ER_XAER_DUPID(ErrorType.Net, 1440),
    ER_DATETIME_FUNCTION_OVERFLOW(ErrorType.Net, 1441),
    ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG(ErrorType.Net, 1442),
    ER_VIEW_PREVENT_UPDATE(ErrorType.Net, 1443),
    ER_PS_NO_RECURSION(ErrorType.Net, 1444),
    ER_SP_CANT_SET_AUTOCOMMIT(ErrorType.Net, 1445),
    ER_NO_VIEW_USER(ErrorType.Net, 1446),
    ER_VIEW_FRM_NO_USER(ErrorType.Net, 1447),
    ER_VIEW_OTHER_USER(ErrorType.Net, 1448),
    ER_NO_SUCH_USER(ErrorType.Net, 1449),
    ER_FORBID_SCHEMA_CHANGE(ErrorType.Net, 1450),
    ER_ROW_IS_REFERENCED_2(ErrorType.Net, 1451),
    ER_NO_REFERENCED_ROW_2(ErrorType.Net, 1452),
    ER_SP_BAD_VAR_SHADOW(ErrorType.Net, 1453),
    ER_PARTITION_REQUIRES_VALUES_ERROR(ErrorType.Net, 1454),
    ER_PARTITION_WRONG_VALUES_ERROR(ErrorType.Net, 1455),
    ER_PARTITION_MAXVALUE_ERROR(ErrorType.Net, 1456),
    ER_PARTITION_SUBPARTITION_ERROR(ErrorType.Net, 1457),
    ER_PARTITION_WRONG_NO_PART_ERROR(ErrorType.Net, 1458),
    ER_PARTITION_WRONG_NO_SUBPART_ERROR(ErrorType.Net, 1459),
    ER_CONST_EXPR_IN_PARTITION_FUNC_ERROR(ErrorType.Net, 1460),
    ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR(ErrorType.Net, 1461),
    ER_FIELD_NOT_FOUND_PART_ERROR(ErrorType.Net, 1462),
    ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR(ErrorType.Net, 1463),
    ER_INCONSISTENT_PARTITION_INFO_ERROR(ErrorType.Net, 1464),
    ER_PARTITION_FUNC_NOT_ALLOWED_ERROR(ErrorType.Net, 1465),
    ER_PARTITIONS_MUST_BE_DEFINED_ERROR(ErrorType.Net, 1466),
    ER_RANGE_NOT_INCREASING_ERROR(ErrorType.Net, 1467),
    ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR(ErrorType.Net, 1468),
    ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR(ErrorType.Net, 1469),
    ER_PARTITION_ENTRY_ERROR(ErrorType.Net, 1470),
    ER_MIX_HANDLER_ERROR(ErrorType.Net, 1471),
    ER_PARTITION_NOT_DEFINED_ERROR(ErrorType.Net, 1472),
    ER_TOO_MANY_PARTITIONS_ERROR(ErrorType.Net, 1473),
    ER_SUBPARTITION_ERROR(ErrorType.Net, 1474),
    ER_CANT_CREATE_HANDLER_FILE(ErrorType.Net, 1475),
    ER_BLOB_FIELD_IN_PART_FUNC_ERROR(ErrorType.Net, 1476),
    ER_CHAR_SET_IN_PART_FIELD_ERROR(ErrorType.Net, 1477),
    ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF(ErrorType.Net, 1478),
    ER_NO_PARTS_ERROR(ErrorType.Net, 1479),
    ER_PARTITION_MGMT_ON_NONPARTITIONED(ErrorType.Net, 1480),
    ER_DROP_PARTITION_NON_EXISTENT(ErrorType.Net, 1481),
    ER_DROP_LAST_PARTITION(ErrorType.Net, 1482),
    ER_COALESCE_ONLY_ON_HASH_PARTITION(ErrorType.Net, 1483),
    ER_ONLY_ON_RANGE_LIST_PARTITION(ErrorType.Net, 1484),
    ER_ADD_PARTITION_SUBPART_ERROR(ErrorType.Net, 1485),
    ER_ADD_PARTITION_NO_NEW_PARTITION(ErrorType.Net, 1486),
    ER_COALESCE_PARTITION_NO_PARTITION(ErrorType.Net, 1487),
    ER_REORG_PARTITION_NOT_EXIST(ErrorType.Net, 1488),
    ER_SAME_NAME_PARTITION(ErrorType.Net, 1489),
    ER_CONSECUTIVE_REORG_PARTITIONS(ErrorType.Net, 1490),
    ER_REORG_OUTSIDE_RANGE(ErrorType.Net, 1491),
    ER_DROP_PARTITION_FAILURE(ErrorType.Net, 1492),
    ER_DROP_PARTITION_WHEN_FK_DEFINED(ErrorType.Net, 1493),
    ER_PLUGIN_IS_NOT_LOADED(ErrorType.Net, 1494),
    ERR_CANT_CHANGE_TX_ISOLATION(ErrorType.Net, 25001),
    XA_RBDEADLOCK(ErrorType.Net, 1614),
    ER_VARIABLE_IS_READONLY(ErrorType.Net, 1621),
    ER_SET_TABLE_READONLY(ErrorType.Net, 1630),
    // ============ config 从4000下标开始==============
    //
    ERR_CONFIG(ErrorType.Config, 4000),

    ERR_MISS_GROUPKEY(ErrorType.Config, 4001),

    ERR_MISS_RULE(ErrorType.Config, 4002),

    ERR_MISS_TOPOLOGY(ErrorType.Config, 4003),

    ERR_MISS_PASSWD(ErrorType.Config, 4004),

    ERR_MISS_ATOM_APP_CONFIG(ErrorType.Config, 4005),

    ERR_TABLE_NOT_EXIST(ErrorType.Config, 4006),

    ERR_CANNOT_FETCH_TABLE_META(ErrorType.Config, 4007),

    ERR_MISS_ATOM_GLOBAL_CONFIG(ErrorType.Config, 4008),

    ERR_MISS_ATOM_OTHER_CONFIG(ErrorType.Config, 4009),

    ERR_PASSWD_DECODE(ErrorType.Config, 4010),

    ERR_VERSION_TOO_LOW(ErrorType.Config, 4011),

    ERR_DUPLICATED_CLASS(ErrorType.Config, 4012),

    ERR_NOT_PASS_RULE_VALIDATE(ErrorType.Config, 4013),

    ERR_NOT_SET_GROUPKEY(ErrorType.Config, 4014),

    ERR_NOT_SET_APPNAME(ErrorType.Config, 4015),

    ERR_MAPPING_RULE_ALREADY_EXISTS(ErrorType.Config, 4016),

    ERR_RULE_PROPERTY_NOT_ALLOWED_TO_CHANGE(ErrorType.Config, 4017),

    ERR_INVALID_DDL_PARAMS(ErrorType.Executor, 4018),

    ERR_ATOM_NOT_AVALILABLE(ErrorType.Atom, 4100),

    ERR_ATOM_GET_CONNECTION_FAILED_UNKNOWN_REASON(ErrorType.Atom, 4101),

    ERR_ATOM_GET_CONNECTION_FAILED_KNOWN_REASON(ErrorType.Atom, 4102),

    ERR_ATOM_CONNECTION_POOL_FULL(ErrorType.Atom, 4103),

    ERR_ATOM_CREATE_CONNECTION_TOO_SLOW(ErrorType.Atom, 4104),

    ERR_ATOM_ACCESS_DENIED(ErrorType.Atom, 4105),

    ERR_ATOM_DB_DOWN(ErrorType.Atom, 4106),

    ERR_SLAVE_DOWN(ErrorType.Atom, 4107),

    ERR_VARIABLE_CAN_NOT_SET_TO_NULL_FOR_NOW(ErrorType.Atom, 4108),

    ERR_GROUP_NOT_AVALILABLE(ErrorType.Group, 4200),

    ERR_GROUP_NO_ATOM_AVAILABLE(ErrorType.Group, 4201),

    ERR_SQL_QUERY_TIMEOUT(ErrorType.Group, 4202),

    ERR_SQL_QUERY_MERGE_TIMEOUT(ErrorType.Group, 4203),

    ERR_SQLFORBID(ErrorType.Group, 4204),

    ERR_HOT_GROUP_NOT_EXISTS(ErrorType.Group, 4205),

    ERR_HOT_TABLE_NAME_WRONG_PATTERN(ErrorType.Group, 4206),

    ERR_ROUTE(ErrorType.Route, 4300),

    ERR_ROUTE_COMPARE_DIFF(ErrorType.Route, 4301),

    ERR_ROUTE_MSHA_UNIT_PARAMS_INVALID(ErrorType.Route, 4302),

    ERR_ROUTE_MSHA_WRITE_SQL_UNIT_NOT_EQUAL_DB_UNIT(ErrorType.Route, 4303),

    ERR_ROUTE_MSHA_WRITE_SQL_FORBID_WRITE(ErrorType.Route, 4304),

    ERR_ROUTE_MSHA_WRITE_SQL_FORBID_UPDATE(ErrorType.Route, 4305),

    ERR_SEQUENCE(ErrorType.Sequence, 4400),

    ERR_MISS_SEQUENCE(ErrorType.Sequence, 4401),

    ERR_MISS_SEQUENCE_DEFAULT_DB(ErrorType.Sequence, 4402),

    ERR_MISS_SEQUENCE_TABLE_ON_DEFAULT_DB(ErrorType.Sequence, 4403),

    ERR_SEQUENCE_TABLE_META(ErrorType.Sequence, 4404),

    ERR_INIT_SEQUENCE_FROM_DB(ErrorType.Sequence, 4405),

    ERR_LOAD_SEQUENCE_FROM_DB(ErrorType.Sequence, 4406),

    ERR_OTHER_WHEN_BUILD_SEQUENCE(ErrorType.Sequence, 4407),

    ERR_SEQUENCE_NEXT_VALUE(ErrorType.Sequence, 4408),

    ERR_PARSER(ErrorType.Parser, 4500), ERR_FASTSQL_PARSER(ErrorType.Parser, 4531),

    ERR_OPTIMIZER(ErrorType.Optimizer, 4501),

    ERR_OPTIMIZER_MISS_ORDER_FUNCTION_IN_SELECT(ErrorType.Optimizer, 4502),

    ERR_OPTIMIZER_MISS_JOIN_FILTER(ErrorType.Optimizer, 4503),

    ERR_OPTIMIZER_SELF_CROSS_JOIN(ErrorType.Optimizer, 4504),

    ERR_MODIFY_SHARD_COLUMN(ErrorType.Optimizer, 4506),

    ERR_MODIFY_PRIMARY_KEY(ErrorType.Optimizer, 4505),

    ERR_SELECT_FROM_UPDATE(ErrorType.Optimizer, 4507),

    ERR_OPTIMIZER_NOT_ALLOWED_SORT_MERGE_JOIN(ErrorType.Optimizer, 4508),

    ERR_OPTIMIZER_ERROR_HINT(ErrorType.Optimizer, 4509),

    ERR_CONTAINS_NO_SHARDING_KEY(ErrorType.Optimizer, 4510),

    ERR_INSERT_CONTAINS_NO_SHARDING_KEY(ErrorType.Optimizer, 4511),

    ERR_DEFAULT_DB_INDEX_IS_NULL(ErrorType.Optimizer, 4512),

    ERR_TABLE_NO_RULE(ErrorType.Optimizer, 4513),

    ERR_RULE_NO_ABS(ErrorType.Optimizer, 4514),

    ERR_CONNECTION_CHARSET_NOT_MATCH(ErrorType.Parser, 4515),

    ERR_UNKNOWN_TZ(ErrorType.Optimizer, 4595),

    ERR_TRUNCATED_DOUBLE_VALUE_OVERFLOW(ErrorType.Optimizer, 4516),

    ERR_MODIFY_SYSTEM_TABLE(ErrorType.Optimizer, 4517),

    ERR_VALIDATE(ErrorType.Optimizer, 4518),

    ERR_MORE_AGG_WITH_DISTINCT(ErrorType.Optimizer, 4519),

    ERR_DML_WITH_SUBQUERY(ErrorType.Optimizer, 4520),

    ERR_INSERT_SHARD(ErrorType.Optimizer, 4521),

    ERROR_MERGE_UPDATE_WITH_LIMIT(ErrorType.Optimizer, 4522),

    ERR_TODNF_LIMIT_EXCEED(ErrorType.Optimizer, 4523),

    ERR_TOCNF_LIMIT_EXCEED(ErrorType.Optimizer, 4524),

    ERR_PK_WRITER_ON_TABLE_WITHOUT_PK(ErrorType.Optimizer, 4525),

    ERR_FUNCTION_NOT_FOUND(ErrorType.Optimizer, 4526, "No match found for function signature .*"),

    ERR_MODIFY_SHARD_COLUMN_ON_TABLE_WITHOUT_PK(ErrorType.Optimizer, 4527),

    /**
     * Hint：Scan、node等hint用法不正确时，可能导致sql的目标表为空，执行结果与预期容易产生误解
     * 例：1.\/*+TDDL:scan(node='10')*\/insert into sbtest1(k) values(30); 库不存在，返回affectRows为0
     * 2.\/*+TDDL:scan(node='10')*\/select * from sbtest1;  库不存在，报错cannot be invoked directly
     * 3.\/*+TDDL:scan()*\/insert into sbtest1 select * from sbtest2;  sbtest1与sbtest2库分布不一样，没法下推，返回affectRows为0
     */
    ERR_TABLE_EMPTY_WITH_HINT(ErrorType.Optimizer, 4528),

    ERR_PARTITION_HINT(ErrorType.Optimizer, 4529),

    // DO NOT support subQuery with correlate rexcall in join condition
    ERR_SUBQUERY_WITH_CORRELATE_CALL_IN_JOIN_CONDITION(ErrorType.Optimizer, 4530),

    ERR_ROWCOUNT_COLLECT(ErrorType.Optimizer, 4530),

    ERR_STATISTIC_JOB_INTERRUPTED(ErrorType.Optimizer, 4531),

    // ============= executor 从4600下标开始================
    //
    ERR_FUNCTION(ErrorType.Executor, 4600),

    ERR_EXECUTOR(ErrorType.Executor, 4601),

    ERR_CONVERTOR(ErrorType.Executor, 4602),

    ERR_ACCROSS_DB_TRANSACTION(ErrorType.Executor, 4603),

    ERR_CONCURRENT_TRANSACTION(ErrorType.Executor, 4604),

    ERR_ROLLBACK_AUTOCOMMIT_TRANSACTION(ErrorType.Executor, 4605),

    ERR_QUERY_CANCLED(ErrorType.Executor, 4606),

    ERR_INSERT_WHEN_UPDATE(ErrorType.Executor, 4607),

    ERR_DELETE_WHEN_UPDATE(ErrorType.Executor, 4608),

    ERR_DUPLICATE_ENTRY(ErrorType.Executor, 4609),

    ERR_CONNECTION_CLOSED(ErrorType.Executor, 4610),

    ERR_UNKNOWN_SAVEPOINT(ErrorType.Executor, 1305),

    ERR_UNKNOWN_THREAD_ID(ErrorType.Executor, 1094),

    ERR_RESULT_DATA(ErrorType.Executor, 4611),

    ERR_CHECK_SQL_PRIV(ErrorType.Executor, 4612),

    ERR_INSERT_SELECT(ErrorType.Executor, 4613),

    ERR_EXECUTE_ON_MYSQL(ErrorType.Executor, 4614),

    ERR_CROSS_JOIN_SIZE_PROTECTION(ErrorType.Executor, 4615),

    ERR_UNKNOWN_DATABASE(ErrorType.Executor, 4616),

    ERR_SUBQUERY_LIMIT_PROTECTION(ErrorType.Executor, 4617),

    ERR_NO_DB_ERROR(ErrorType.Executor, 4618),

    ERR_EXECUTE_ON_MYSQL_UNKNOWN_COLUMN(ErrorType.Executor, 4619),

    ERR_FORBID_EXECUTE_DML_ALL(ErrorType.Executor, 4620),

    ERR_REPLACE_SELECT(ErrorType.Executor, 4621),

    ERR_ORIGIN_STMT_UNEXPECTED_CONST(ErrorType.Executor, 4625),

    ERR_PARAM_COUNT_NOT_EQUAL(ErrorType.Executor, 4626),

    ERR_TARGET_STMT_UNEXPECTED_PARAM(ErrorType.Executor, 4627),

    ERR_ORIGIN_STMT_CONFLICTED(ErrorType.Executor, 4628),

    ERR_TARGET_STMT_ERROR(ErrorType.Executor, 4629),

    ERR_RECYCLEBIN_EXECUTE(ErrorType.Executor, 4630),

    ERR_INSERT_SELECT_LIMIT_EXCEEDED(ErrorType.Executor, 4631),

    ERR_DB_STATUS_EXECUTE(ErrorType.Executor, 4632),

    ERR_DB_STATUS_READ_ONLY(ErrorType.Executor, 4633),

    ERR_NO_FOUND_DATASOURCE(ErrorType.Executor, 4634),

    ERR_FORBID_EXEC_AP_DML(ErrorType.Executor, 4635),

    ERR_DDL_JOB_ERROR(ErrorType.Executor, 4636),

    ERR_DDL_JOB_FAILED(ErrorType.Executor, 4637),

    ERR_DDL_JOB_UNEXPECTED(ErrorType.Executor, 4638),

    ERR_DDL_JOB_UNSUPPORTED(ErrorType.Executor, 4639),

    ERR_DDL_JOB_INVALID(ErrorType.Executor, 4640),

    ERR_DDL_JOB_WARNING(ErrorType.Executor, 4641),

    ERR_UNKNOWN_TABLE(ErrorType.Executor, 4642),

    ERR_UNKNOWN_COLUMN(ErrorType.Executor, 4662),

    ERR_DUPLICATE_COLUMN(ErrorType.Executor, 4663),

    ERR_DUPLICATE_KEY(ErrorType.Executor, 4664),
    ERR_UNKNOWN_KEY(ErrorType.Executor, 4665),
    ERR_MULTIPLE_PRIMARY_KEY(ErrorType.Executor, 4666),
    ERR_ADD_PRIMARY_KEY(ErrorType.Executor, 4667),
    ERR_DROP_PRIMARY_KEY(ErrorType.Executor, 4668),
    ERR_ALTER_SHARDING_KEY(ErrorType.Executor, 4669),
    ERR_DROP_ALL_COLUMNS(ErrorType.Executor, 4670),
    /**
     * the dynamic value of scalar subquery is not ready
     */
    ERR_SUBQUERY_VALUE_NOT_READY(ErrorType.Executor, 4671),

    /**
     * foreign key constraints
     */
    ERR_DUPLICATE_NAME_FK_CONSTRAINT(ErrorType.Executor, 4680),
    ERR_DROP_FK_CONSTRAINT(ErrorType.Executor, 4681),
    ERR_DROP_COLUMN_FK_CONSTRAINT(ErrorType.Executor, 4682),
    ERR_CHANGE_COLUMN_FK_CONSTRAINT(ErrorType.Executor, 4683),
    ERR_TRUNCATE_TABLE_FK_CONSTRAINT(ErrorType.Executor, 4684),
    ERR_DROP_TABLE_FK_CONSTRAINT(ErrorType.Executor, 4685),
    ERR_ADD_UPDATE_FK_CONSTRAINT(ErrorType.Executor, 4686),
    ERR_CREATE_FK_MISSING_INDEX(ErrorType.Executor, 4687),
    ERR_ADD_FK_CONSTRAINT(ErrorType.Executor, 4688),
    ERR_DROP_FK_INDEX(ErrorType.Executor, 4689),
    ERR_RENAMES_TABLE_FK_CONSTRAINT(ErrorType.Executor, 4690),
    ERR_FK_EXCEED_MAX_DEPTH(ErrorType.Executor, 4691),
    ERR_DROP_PARTITION_FK_CONSTRAINT(ErrorType.Executor, 4692),
    ERR_ADD_FK_ENGINE(ErrorType.Executor, 4693),
    ERR_ADD_FK_GENERATED_COLUMN(ErrorType.Executor, 4694),
    ERR_ADD_FK_CHARSET_COLLATION(ErrorType.Executor, 4695),
    ERR_FK_CONVERT_TO_CHARSET(ErrorType.Executor, 4696),
    ERR_FK_REFERENCING_COLUMN_NOT_EXIST(ErrorType.Executor, 4697),

    ERR_UNSUPPORTED_COLUMN_TYPE_WITH_CCI(ErrorType.Executor, 4698),
    ERR_DDL_WITH_CCI(ErrorType.Executor, 4699),

    ERR_TABLE_ALREADY_EXISTS(ErrorType.Executor, 4643),

    ERR_PENDING_DDL_JOB_EXISTS(ErrorType.Executor, 4644),

    ERR_DDL_JOB_INTERRUPTED(ErrorType.Executor, 4645),

    ERR_PENDING_DDL_JOBS_EXCEED_LIMIT(ErrorType.Executor, 4646),

    ERR_TABLE_PARTITIONS_EXCEED_LIMIT(ErrorType.Executor, 4647),

    ERR_DROP_DB_NOT_EXISTS(ErrorType.Executor, 4648),

    ERR_DROP_DB_ILLEGAL_STATE(ErrorType.Executor, 4649),

    ERR_USER_LOCK_DEADLOCK(ErrorType.Executor, 4650),

    ERR_UPDATE_DELETE_SELECT_LIMIT_EXCEEDED(ErrorType.Executor, 4650),

    ERR_UPDATE_DELETE_NO_PRIMARY_KEY(ErrorType.Executor, 4651),

    ERR_SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_ROW(ErrorType.Executor, 4652),

    ERR_PARTITION_RULE_UNCHANGED(ErrorType.Executor, 4653),

    ERR_PARTITION_RECOVER_UNSUPPORTED(ErrorType.Executor, 4654),

    ERR_PARTITION_ROLLBACK_UNSUPPORTED(ErrorType.Executor, 4655),

    ERR_PARTITION_UNKNOWN_GSI_NAME(ErrorType.Executor, 4656),

    ERR_UPDATE_PRIMARY_KEY_WITH_NULL_OR_ZERO(ErrorType.Executor, 4657),

    ERR_PARTITION_GSI_MISSING_COLUMN(ErrorType.Executor, 4658),

    ERR_PARTITION_MISSING_SEQUENCE(ErrorType.Executor, 4659),

    ERR_PARTITION_WITH_NON_PUBLIC_GSI(ErrorType.Executor, 4660),

    ERR_PARTITION_TO_SINGLE_OR_BROADCAST_WITH_GSI(ErrorType.Executor, 4661),

    ERR_PAUSED_DDL_JOB_EXISTS(ErrorType.Executor, 4662),

    ERR_TABLE_META_TOO_OLD(ErrorType.Executor, 4663),

    ERR_SET_AUTO_SAVEPOINT(ErrorType.Executor, 4664),

    ERR_CREATE_SELECT_UPDATE(ErrorType.Executor, 4665),
    ERR_CREATE_SELECT_FUNCTION_ALIAS(ErrorType.Executor, 4666),

    ERR_CREATE_SELECT_WITH_GSI(ErrorType.Executor, 4667),

    ERR_CREATE_SELECT_WITH_OSS(ErrorType.Executor, 4668),

    // ============= server 从4700下标开始================
    ERR_SERVER(ErrorType.Server, 4700),

    ERR_RPC(ErrorType.Server, 4701),

    ERR_NET_SEND(ErrorType.Server, 4702),

    ERR_PACKET_COMPOSE(ErrorType.Server, 4703),

    ERR_PACKET_READ(ErrorType.Server, 4704),

    ERR_PACKET_SSL_SEND(ErrorType.Server, 4706),

    ERR_OUT_OF_MEMORY(ErrorType.Executor, 4707),

    ERR_OPERATION_COMMIT_ON_SUCCESS_NOT_ALLOWED(ErrorType.Executor, 4708),

    ERR_IVENTORY_HINT_NOT_SUPPORT_CROSS_SHARD(ErrorType.Executor, 4709),

    ERR_FLOW_CONTROL(ErrorType.Other, 4994),

    ERR_ASSERT_NULL(ErrorType.Other, 4995),

    ERR_ASSERT_TRUE(ErrorType.Other, 4996),

    ERR_ASSERT_FAIL(ErrorType.Other, 4997),

    ERR_NOT_SUPPORT(ErrorType.Other, 4998),

    ERR_OTHER(ErrorType.Other, 4999),

    ERR_TRANS(ErrorType.Transaction, 5001),

    ERR_TRANS_UNSUPPORTED(ErrorType.Transaction, 5002),

    ERR_TRANS_LOG(ErrorType.Transaction, 5003),

    ERR_TRANS_NOT_FOUND(ErrorType.Transaction, 5004),

    ERR_TRANS_FINISHED(ErrorType.Transaction, 5005),

    ERR_TRANS_COMMIT(ErrorType.Transaction, 5006),

    ERR_TRANS_PARAM(ErrorType.Transaction, 5007),

    ERR_TRANS_TERMINATED(ErrorType.Transaction, 5008),

    ERR_TRANS_DEADLOCK(ErrorType.Transaction, 5009),

    ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL(ErrorType.Transaction, 5010),

    ERR_TRANS_DISTRIBUTED_TRX_REQUIRED(ErrorType.Transaction, 5011),

    ERR_TRANS_PREEMPTED_BY_DDL(ErrorType.Transaction, 5012),

    ERR_TRANS_CANNOT_EXECUTE_IN_RO_TRX(ErrorType.Transaction, 5013),

    // 回滚单个语句失败
    ERR_TRANS_ROLLBACK_STATEMENT_FAIL(ErrorType.Transaction, 5014),

    // 事务中获取分库内并行写连接失败
    ERR_TRANS_FETCH_INTRA_GROUP_CONN_ID_FAIL(ErrorType.Transaction, 5015),

    // 事务超时
    ERR_TRANS_IDLE_TIMEOUT(ErrorType.Transaction, 5016),
    // ================权限相关异常从5101开始==================
    /**
     * 暂时不支持的权限点
     */
    ERR_UNSUPPORTED_PRIVILEGE(ErrorType.Priviledge, 5101),

    ERR_GRANT_PRIVILEGE_FAILED(ErrorType.Priviledge, 5102),

    ERR_REVOKE_PRIVILEGE_FAILED(ErrorType.Priviledge, 5103),

    ERR_GRANT_NONEXISTING_USER(ErrorType.Priviledge, 5104),

    ERR_SYNC_PRIVILEGE_FAILED(ErrorType.Priviledge, 5105),

    ERR_NO_ACCESS_TO_DATABASE(ErrorType.Priviledge, 5106),

    ERR_OPERATION_NOT_ALLOWED(ErrorType.Priviledge, 5107),

    ERR_CHECK_PRIVILEGE_FAILED_ON_TABLE(ErrorType.Priviledge, 5108),

    ERR_INVALID_HOST(ErrorType.Priviledge, 5109),

    ERR_CHECK_PRIVILEGE_FAILED(ErrorType.Priviledge, 5110),

    AUTHORITY_SUCCESS(ErrorType.Priviledge, 5111),

    AUTHORITY_COMMON_EXCEPTION(ErrorType.Priviledge, 5112),

    REVOKE_NO_SUCH_PRIVILEGE_EXCEPTION(ErrorType.Priviledge, 5113),

    ERR_GRANTER_NO_GRANT_PRIV(ErrorType.Priviledge, 5114),

    ERR_GRANT_NOT_ALLOWED_IN_APMODE(ErrorType.Priviledge, 5115),

    ERR_EXCEED_MAX_EXECUTE_MEMORY(ErrorType.Executor, 5116),

    ERR_DROP_INFORMATION_SCHEMA(ErrorType.Priviledge, 5117),

    ERR_ROLE_NOT_GRANTED(ErrorType.Priviledge, 5118),

    ERR_CHECK_PRIVILEGE_FAILED_ON_DB(ErrorType.Priviledge, 5118),

    ERR_FILE_CANNOT_BE_CREATE(ErrorType.Priviledge, 5119),

    ERR_FILE_ALREADY_EXIST(ErrorType.Priviledge, 5120),

    ERR_CHARACTER_NOT_SUPPORT(ErrorType.Priviledge, 5121),

    ERR_DATATYPE_NOT_SUPPORT(ErrorType.Priviledge, 5122),
    ERR_INSTANCE_READ_ONLY_OPTION_NOT_SUPPORT(ErrorType.Priviledge, 5123),

    ERR_ENCDB(ErrorType.Priviledge, 5123),

    ERR_LBAC(ErrorType.Priviledge, 5124),

    ERR_CREATE_USER_FAILED(ErrorType.Account, 5200),

    ERR_DROP_USER_FAILED(ErrorType.Account, 5201),

    ERR_INVALID_USERNAME(ErrorType.Account, 5202),

    /**
     * 密码不符合默认规则
     */
    ERR_INVALID_PASSWORD(ErrorType.Account, 5203),

    ERR_USER_ALREADY_EXISTS(ErrorType.Account, 5204),

    ERR_ACCOUNT_LIMIT_EXCEEDED(ErrorType.Account, 5205),

    ERR_USER_NOT_EXISTS(ErrorType.Account, 5206),

    /**
     * 密码不符合自定义规则
     */
    ERR_INVALID_PASSWORD_CUSTOMIZED(ErrorType.Account, 5207),

    // ================全局二级索引相关异常从5300开始==================

    ERR_GLOBAL_SECONDARY_INDEX_EXECUTE(ErrorType.Executor, 5301),

    ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED(ErrorType.Executor, 5302),

    ERR_GLOBAL_SECONDARY_INDEX_UPDATE_NUM_EXCEEDED(ErrorType.Executor, 5303),

    ERR_GLOBAL_SECONDARY_INDEX_KEY_DEFAULT(ErrorType.Executor, 5304),

    ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT(ErrorType.Executor, 5305),

    ERR_GLOBAL_SECONDARY_INDEX_INSERT_DUPLICATE_VALUES(ErrorType.Executor, 5306),

    ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL(ErrorType.Executor, 5307),

    ERR_GLOBAL_SECONDARY_INDEX_MODIFY_UNIQUE_KEY(ErrorType.Executor, 5308),

    @Deprecated
    ERR_GLOBAL_SECONDARY_INDEX_UPDATE_DELETE_MULTI_TABLE(ErrorType.Executor, 5309),

    ERR_GLOBAL_SECONDARY_INDEX_ONLY_SUPPORT_XA(ErrorType.Executor, 5310),

    ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_DIRECTLY(ErrorType.Executor, 5311),

    ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_PRIMARY_TABLE_DIRECTLY(ErrorType.Executor, 5312),

    ERR_GLOBAL_SECONDARY_INDEX_MODIFY_GSI_TABLE_WITH_DDL(ErrorType.Optimizer, 5313),

    ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_PRIMARY_TABLE_DEFINITION(ErrorType.Optimizer, 5314),

    ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_STORAGE_VERSION(ErrorType.Executor, 5315),

    ERR_GLOBAL_SECONDARY_INDEX_INDEX_AND_SHARDING_COLUMNS_NOT_MATCH(ErrorType.Optimizer, 5316),

    ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL(ErrorType.Executor, 5317),

    ERR_GLOBAL_SECONDARY_INDEX_TRUNCATE_PRIMARY_TABLE(ErrorType.Optimizer, 5318),

    ERR_GLOBAL_SECONDARY_INDEX_ALLOW_ADD(ErrorType.Optimizer, 5319),

    ERR_GLOBAL_SECONDARY_INDEX_UNSUPPORTED_INDEX_TABLE_DEFINITION(ErrorType.Optimizer, 5320),

    ERR_GLOBAL_SECONDARY_INDEX_BACKFILL_DUPLICATE_ENTRY(ErrorType.Executor, 5321),

    ERR_GLOBAL_SECONDARY_INDEX_CHECKER(ErrorType.Executor, 5322),

    ERR_CLUSTERED_INDEX_ADD_COLUMNS(ErrorType.Executor, 5323),
    /**
     * FastChecker 校验失败
     */
    ERR_FAST_CHECKER(ErrorType.Executor, 5324),

    /**
     * Modifying a broadcast table is not allowed
     */
    ERR_MODIFY_BROADCAST_TABLE_BY_HINT_NOT_ALLOWED(ErrorType.Executor, 5325),

    /**
     * error for auto partition table
     */
    ERR_AUTO_PARTITION_TABLE(ErrorType.Executor, 5326),

    ERR_BACKFILL_GET_TABLE_ROWS(ErrorType.Executor, 5327),

    // ================拆分键推荐相关异常==================

    ERR_KEY_NEGATIVE(ErrorType.Other, 5401),

    ERR_EDGE_REVERSED(ErrorType.Other, 5402),

    ERR_CANDIDATE_NOT_CLEAR(ErrorType.Other, 5403),

    ERR_RESULT_WEIGHT_FAULT(ErrorType.Other, 5404),

    ERR_CUT_WRONG(ErrorType.Other, 5405),

    ERR_GRAPH_TOO_BIG(ErrorType.Other, 5406),

    ERR_REMOVE_USELESS_SHARD(ErrorType.Other, 5407),

    ERR_SINGLE_SHARD_PLAN(ErrorType.Other, 5408),

    ERR_CANT_GET_CACHE(ErrorType.Other, 5409),

    ERR_UNEXPECTED_SQL(ErrorType.Other, 5410),

    ERR_CANT_FIND_COLUMN(ErrorType.Other, 5411),

    // ================存储过程相关异常================
    ERR_UNEXPECTED_STATEMENT_TYPE(ErrorType.Procedure, 5500),

    ERR_NO_MATCHED_EXCEPTION_HANDLER(ErrorType.Procedure, 5501),

    ERR_UNEXPECTED_HANDLER_TYPE(ErrorType.Procedure, 5502),

    ERR_NOT_SUPPORT_STATEMENT_TYPE(ErrorType.Procedure, 5503),

    ERR_UDF_CANNOT_SET_NON_PL_VARIABLE(ErrorType.Procedure, 5504),

    ERR_PROCEDURE_EXECUTE(ErrorType.Procedure, 5505),

    ERR_PROCEDURE_NOT_FOUND(ErrorType.Procedure, 5506),

    ERR_PROCEDURE_PARAMS_NOT_MATCH(ErrorType.Procedure, 5507),

    ERR_PROCEDURE_LOAD_FAILED(ErrorType.Procedure, 5509),

    ERR_UDF_RECURSIVE_CALLED(ErrorType.Function, 5510),

    ERR_UDF_NOT_FOUND(ErrorType.Function, 5511),

    ERR_PROCEDURE_ALREADY_EXISTS(ErrorType.Procedure, 5512),

    ERR_UDF_ALREADY_EXISTS(ErrorType.Function, 5513),

    ERR_UDF_EXECUTE(ErrorType.Function, 5514),

    ERR_UDF_NOT_SUPPORT(ErrorType.Function, 5515),

    ERR_EXCEPTION_TYPE_NOT_SUPPORT(ErrorType.Procedure, 5515),

    ERR_DATA_NOT_FOUND(ErrorType.Procedure, 5516),

    // ================列存索引相关异常从5600开始==================
    /**
     * 列存索引校验相关错误
     */
    ERR_COLUMNAR_INDEX_CHECKER(ErrorType.Executor, 5600),

    // ================鉴权相关异常==================

    ERR_AUTH_AKSK_FAIL(ErrorType.Auth, 6001),

    ERR_BASELINE(ErrorType.Baseline, 7001),
    ERR_PLAN_COST(ErrorType.Baseline, 7002),

    // ================View 异常 7901 - 8000==================
    ERR_VIEW(ErrorType.Executor, 7901),

    ERR_ABANDONED_QUERY(ErrorType.Executor, 8001),

    ERR_USER_CANCELED(ErrorType.Executor, 8002),

    ERR_NOT_FOUND_ROOT_PLAN(ErrorType.Executor, 8003),

    ERR_KILLED_QUERY(ErrorType.Executor, 8004),

    ERR_GENERATE_PLAN(ErrorType.Executor, 8005),

    ERR_GENERATE_SPLIT(ErrorType.Executor, 8006),

    ERR_ABANDONED_TASK(ErrorType.Executor, 8007),

    ERR_EXECUTE_SPILL(ErrorType.Executor, 8008),

    ERR_SERVER_SHUTTING_DOWN(ErrorType.Executor, 8009),

    ERR_CORRUPT_PAGE(ErrorType.Executor, 8010),

    ERR_OUT_OF_SPILL_SPACE(ErrorType.Executor, 8011),

    ERR_OUT_OF_SPILL_FD(ErrorType.Executor, 8012),

    ERR_DATA_OUTPUT(ErrorType.Executor, 8013),

    ERR_EXECUTE_MPP(ErrorType.Mpp, 8101),

    ERR_PAGE_TOO_LARGE(ErrorType.Mpp, 8102),

    ERR_PAGE_TRANSPORT_ERROR(ErrorType.Mpp, 8103),

    ERR_PAGE_TRANSPORT_TIMEOUT(ErrorType.Mpp, 8104),

    ERR_NO_NODES_AVAILABLE(ErrorType.Mpp, 8105),

    ERR_REMOTE_TASK(ErrorType.Mpp, 8106),

    ERR_REMOTE_BUFFER(ErrorType.Mpp, 8107),

    RPC_REFUSED_CONNECTION_ERROR(ErrorType.Mpp, 8027),

    ERR_GMS_GENERIC(ErrorType.GMS, 9001),

    ERR_GMS_UNSUPPORTED(ErrorType.GMS, 9002),

    ERR_GMS_UNEXPECTED(ErrorType.GMS, 9003),

    ERR_GMS_GET_CONNECTION(ErrorType.GMS, 9004),

    ERR_GMS_CHECK_ARGUMENTS(ErrorType.GMS, 9005),

    ERR_GMS_ACCESS_TO_SYSTEM_TABLE(ErrorType.GMS, 9006),

    ERR_GMS_INIT_ROOT_DB(ErrorType.GMS, 9007),

    ERR_GMS_SCHEMA_CHANGE(ErrorType.GMS, 9008),

    ERR_GMS_MAINTAIN_TABLE_META(ErrorType.GMS, 9009),

    ERR_GMS_NEW_SEQUENCE(ErrorType.GMS, 9010),

    // ================= ScaleOut Related Exceptions ===================

    ERR_SCALEOUT_EXECUTE(ErrorType.Executor, 9101),
    ERR_SCALEOUT_CHECKER(ErrorType.Executor, 9102),

    ERR_REBALANCE(ErrorType.Executor, 9103),

    ERR_CHANGESET(ErrorType.Executor, 9104),

    // ================= concurrency control Related Exceptions ===================

    ERR_CCL(ErrorType.CCL, 9201),

    ERR_LOGICAL_TABLE_UNSUPPORTED(ErrorType.Executor, 9203),

    ERR_CDC_GENERIC(ErrorType.CDC, 9201),

    ERR_REPLICATION_RESULT(ErrorType.CDC, 9204),

    ERR_REPLICA_NOT_SUPPORT(ErrorType.CDC, 9205),
    ERR_INSTANCE_READ_ONLY_OPTION_SET_FAILED(ErrorType.CDC, 9206),

    ERR_PARTITION_MANAGEMENT(ErrorType.Executor, 9300),

    ERR_DUPLICATED_PARTITION_NAME(ErrorType.Executor, 9301),
    ERR_ADD_PARTITION(ErrorType.Executor, 9302),
    ERR_DROP_PARTITION(ErrorType.Executor, 9303),
    ERR_TABLE_GROUP_NOT_EXISTS(ErrorType.Executor, 9304),
    ERR_PARTITION_NAME_NOT_EXISTS(ErrorType.Executor, 9305),
    ERR_PARTITION_INVALID_PARAMS(ErrorType.Executor, 9306),
    ERR_TABLE_GROUP_NOT_INIT(ErrorType.Executor, 9307),
    ERR_PARTITION_INVALID_DATA_TYPE_CONVERSION(ErrorType.Executor, 9308),
    ERR_PARTITION_NO_FOUND(ErrorType.Executor, 9309),
    ERR_PARTITION_KEY_DATA_TRUNCATED(ErrorType.Executor, 9310),

    ERR_REPARTITION_KEY(ErrorType.Executor, 9311),
    ERR_REPARTITION_TABLE_WITH_GSI(ErrorType.Executor, 9312),
    ERR_TABLEGROUP_META_TOO_OLD(ErrorType.Executor, 9313),
    ERR_TABLE_GROUP_CHANGED(ErrorType.Executor, 9314),
    ERR_PHYSICAL_TOPOLOGY_CHANGING(ErrorType.Executor, 9315),
    ERR_DN_IS_NOT_READY(ErrorType.Executor, 9316),
    ERR_JOIN_GROUP_ALREADY_EXISTS(ErrorType.Executor, 9317),
    ERR_JOIN_GROUP_NOT_EXISTS(ErrorType.Executor, 9318),
    ERR_JOIN_GROUP_NOT_EMPTY(ErrorType.Executor, 9319),
    ERR_JOIN_GROUP_NOT_MATCH(ErrorType.Executor, 9320),
    ERR_TABLE_GROUP_IS_EMPTY(ErrorType.Executor, 9321),
    ERR_TABLE_NAME_TOO_MANY_HIERARCHY(ErrorType.Executor, 9322),
    ERR_TABLE_GROUP_IS_AUTO_CREATED(ErrorType.Executor, 9323),
    ERR_RENAME_BROADCAST_OR_SINGLE_TABLE(ErrorType.Executor, 9324),
    ERR_CHANGE_TABLEGROUP_FOR_BROADCAST_TABLE(ErrorType.Executor, 9325),
    ERR_PARTITION_COLUMN_IS_NOT_MATCH(ErrorType.Executor, 9326),
    ERR_GLOBAL_SECONDARY_DROP_PARTITION(ErrorType.Executor, 9327),
    ERR_GLOBAL_SECONDARY_MODIFY_PARTITION_DROP_VALUE(ErrorType.Executor, 9328),
    ERR_GLOBAL_SECONDARY_TRUNCATE_PARTITION(ErrorType.Executor, 9329),

    ERR_ADD_SUBPARTITION(ErrorType.Executor, 9327),
    ERR_DROP_SUBPARTITION(ErrorType.Executor, 9328),
    ERR_MODIFY_PARTITION(ErrorType.Executor, 9329),
    ERR_MODIFY_SUBPARTITION(ErrorType.Executor, 9330),
    ERR_SUBPARTITION_NOT_EXISTS(ErrorType.Executor, 9331),
    ERR_TEMPLATE_SUBPARTITION_PARTITION_NOT_EXIST(ErrorType.Executor, 9332),

    ERR_MISS_PARTITION_STRATEGY(ErrorType.Executor, 9333),
    ERR_MISS_SUBPARTITION_STRATEGY(ErrorType.Executor, 9334),
    ERR_SUBPARTITION_STRATEGY_NOT_EXIST(ErrorType.Executor, 9335),
    ERR_PARTITION_STRATEGY_IS_NOT_EQUAL(ErrorType.Executor, 9336),
    ERR_SUBPARTITION_STRATEGY_IS_NOT_EQUAL(ErrorType.Executor, 9337),
    ERR_REDUNDANCY_PARTITION_DEFINITION(ErrorType.Executor, 9338),
    ERR_REDUNDANCY_SUBPARTITION_DEFINITION(ErrorType.Executor, 9339),
    ERR_AUTO_CREATE_TABLEGROUP(ErrorType.Executor, 9340),

    // ============= 私有协议 从10000下标开始================
    ERR_X_PROTOCOL_BAD_PACKET(ErrorType.Xprotocol, 10000),

    ERR_X_PROTOCOL_CLIENT(ErrorType.Xprotocol, 10001),

    ERR_X_PROTOCOL_SESSION(ErrorType.Xprotocol, 10002),

    ERR_X_PROTOCOL_CONNECTION(ErrorType.Xprotocol, 10003),

    ERR_X_PROTOCOL_RESULT(ErrorType.Xprotocol, 10004),
    // ============= OSS ================
    ERR_SHOULD_NOT_BE_NULL(ErrorType.OSS, 11001),
    ERR_BACK_FILL_FAIL(ErrorType.OSS, 11002),
    ERR_BACK_FILL_ROLLBACK_FAIL(ErrorType.OSS, 11003),
    ERR_BACK_FILL_CHECK(ErrorType.OSS, 11004),
    ERR_CANT_CONTINUE_DDL(ErrorType.OSS, 11005),
    ERR_OSS_FORMAT(ErrorType.OSS, 11006),
    ERR_DROP_RECYCLE_BIN(ErrorType.OSS, 11007),
    ERR_UNARCHIVE_FIRST(ErrorType.OSS, 11008),
    ERR_UNEXPECTED_REL_TREE(ErrorType.OSS, 11010),
    ERR_EXECUTE_ON_OSS(ErrorType.Executor, 11011),
    ERR_FILE_STORAGE_READ_ONLY(ErrorType.OSS, 11012),
    ERR_OSS_CONNECT(ErrorType.OSS, 11013),
    ERR_FILE_STORAGE_EXISTS(ErrorType.OSS, 11014),
    ERR_BACK_FILL_TIMEOUT(ErrorType.OSS, 11015),
    ERR_ARCHIVE_NOT_ENABLED(ErrorType.OSS, 11016),
    ERR_ARCHIVE_TABLE_EXISTS(ErrorType.OSS, 11017),

    // ============= Columnar Related Exceptions ================
    ERR_BINARY_PREDICATE(ErrorType.ColumnarIndexPrune, 12000),
    ERR_BITMAP_ROW_GROUP_INDEX(ErrorType.ColumnarIndexPrune, 12001),
    ERR_LOAD_CSV_FILE(ErrorType.ColumnarAccess, 12002),
    ERR_LOAD_DEL_FILE(ErrorType.ColumnarAccess, 12003),
    ERR_LOAD_ORC_FILE(ErrorType.ColumnarAccess, 12004),
    ERR_COLUMNAR_SNAPSHOT(ErrorType.GMS, 12005),
    ERR_COLUMNAR_SCHEMA(ErrorType.GMS, 12006);

    private static final String errorMessagePre = "ERR-CODE: [PXC-";
    private final int code;
    private ErrorType type;
    private Pattern pattern;

    ErrorCode(ErrorType type, int code) {
        this(type, code, null);
    }

    ErrorCode(ErrorType type, int code, String regex) {
        this.code = code;
        this.type = type;
        if (regex != null) {
            this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        }
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return type.name();
    }

    public ErrorType getType() {
        return type;
    }

    public String getMessage(String... params) {
        return ResourceBundleUtil.getInstance().getMessage(this.name(), this.getCode(), this.getName(), params);
    }

    /**
     * Check if the error message conforms to the required error format.
     */
    public static boolean match(String message) {
        return extract(message) == -1 ? false : true;
    }

    /**
     * get error code from error message
     */
    public static int extract(String message) {
        if (message != null) {
            // get error code from message
            if (message.startsWith(errorMessagePre)) {
                int endPos = message.indexOf(']', errorMessagePre.length());
                try {
                    return Integer.parseInt(message.substring(errorMessagePre.length(), endPos));
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return -1;
    }
}

