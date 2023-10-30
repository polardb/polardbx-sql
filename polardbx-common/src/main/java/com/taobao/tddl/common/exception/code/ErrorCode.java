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

package com.taobao.tddl.common.exception.code;

import com.alibaba.polardbx.common.exception.code.ResourceBundleUtil;

import java.util.regex.Pattern;


@Deprecated
public enum ErrorCode {

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

    ERR_CREATE_USER_FAILED(ErrorType.Account, 5200),

    ERR_DROP_USER_FAILED(ErrorType.Account, 5201),

    ERR_INVALID_USERNAME(ErrorType.Account, 5202),

    ERR_INVALID_PASSWORD(ErrorType.Account, 5203),

    ERR_USER_ALREADY_EXISTS(ErrorType.Account, 5204),

    ERR_ACCOUNT_LIMIT_EXCEEDED(ErrorType.Account, 5205),

    ERR_USER_NOT_EXISTS(ErrorType.Account, 5206),

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

    ERR_AUTH_AKSK_FAIL(ErrorType.Auth, 6001),

    ERR_BASELINE(ErrorType.Baseline, 7001),

    ERR_VIEW(ErrorType.Executor, 7900),

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

    ERR_SCALEOUT_EXECUTE(ErrorType.Executor, 9101),
    ERR_SCALEOUT_CHECKER(ErrorType.Executor, 9102),

    ERR_REBALANCE(ErrorType.Executor, 9103),

    ERR_CHANGESET(ErrorType.Executor, 9104),

    // ================= concurrency control Related Exceptions ===================

    ERR_CCL(ErrorType.CCL, 9201),

    ERR_LOGICAL_TABLE_UNSUPPORTED(ErrorType.Executor, 9203),

    ERR_CDC_GENERIC(ErrorType.CDC, 9201),
    ERR_REPLICATION_RESULT(ErrorType.CDC, 9204),

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
    ERR_TABLEGROUP_META_TOO_OLD(ErrorType.Executor, 9313),

    ERR_X_PROTOCOL_BAD_PACKET(ErrorType.Xprotocol, 10000),

    ERR_X_PROTOCOL_CLIENT(ErrorType.Xprotocol, 10001),

    ERR_X_PROTOCOL_SESSION(ErrorType.Xprotocol, 10002),

    ERR_X_PROTOCOL_CONNECTION(ErrorType.Xprotocol, 10003),

    ERR_X_PROTOCOL_RESULT(ErrorType.Xprotocol, 10004);

    private int code;
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

    public static ErrorCode match(String message, ErrorCode defaultCode) {
        for (ErrorCode errorCode : values()) {
            if (errorCode.pattern != null
                && errorCode.pattern.matcher(message).find()) {
                return errorCode;
            }
        }
        return defaultCode;
    }

    public boolean equals(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode) {
        return this.convert().equals(errorCode);
    }

    public com.alibaba.polardbx.common.exception.code.ErrorCode convert() {
        return com.alibaba.polardbx.common.exception.code.ErrorCode.valueOf(this.name());
    }
}

