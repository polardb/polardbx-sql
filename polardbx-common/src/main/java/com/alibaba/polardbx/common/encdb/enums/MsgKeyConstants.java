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

package com.alibaba.polardbx.common.encdb.enums;

public class MsgKeyConstants {
    //no under score for mrenclave, xxid, keyname
    public static final String REQUEST_TYPE = "request_type";
    public static final String MEKID = "mekid";
    public static final String MRENCLAVE = "mrenclave";
    public static final String MRENCLAVE_SIG = "mrenclave_sig";
    public static final String SERVER_INFO = "server_info";
    public static final String SERVER_NONE = "nonce";
    public static final String SERVER_INFO_MAC = "server_info_mac";

    public static final String VERSION = "version";
    public static final String TEE_TYPE = "tee_type";

    public static final String CIPHER_SUITE = "cipher_suite";
    public static final String PUBLIC_KEY = "public_key";
    public static final String PUBLIC_KEY_HASH = "public_key_hash";
    public static final String QUOTE = "quote";
    public static final String ENVELOPE = "envelope";
    public static final String MEK = "mek";
    public static final String MEK_ID = "mekid";
    public static final String EXPIRE = "expire";
    public static final String ACL_LIST = "acl_list";
    public static final String ENVELOPED = "enveloped";

    public static final String OLD_MEK = "old_mek";
    public static final String KMS_MEK = "kms_mek";
    public static final String KEYNAME = "keyname";
    public static final String CIPHER_CONTEXT = "CipherContext";
    public static final String CTXID = "ctxid";

    public static final String DEKID = "dekid";
    public static final String DEK = "dek";
    public static final String ENCRYPTED_DEK = "encrypted_dek";
    public static final String GROUPID = "groupid";
    public static final String GROUP_AUTH = "group_auth";
    public static final String ALGORITHM = "algorithm";
    public static final String POLICY = "policy";
    public static final String FLAGS = "flags";
    public static final String ROTATED = "rotated";

    public static final String OLD_CTXID = "old_ctxid";
    public static final String NEW_CTXID = "new_ctxid";
    public static final String USERNAME = "username"; //get_current_username

    //RA_GET_Quote RA_VERIFY_QUOTE and other related operations
    public static final String CHALLENGE = "challenge";
    public static final String USER_DATA = "user_data";
    public static final String ENCLAVE_DATA = "enclave_data";
    public static final String KEY_META_DATA = "key_meta_data";

    //used by export data key
    public static final String TABLE_NAME = "tableName";
    public static final String COLUMN_TYPE = "columnType";
    public static final String COLUMN_NAME = "columnName";

    // used by BCL
    public static final String ISSUER_MEKID = "issuer_mekid";
    public static final String SUBJECT_MEKID = "subject_mekid";
    public static final String PUKID = "pukid";
    public static final String ISSUER_PUKID = "issuer_pukid";
    public static final String SUBJECT_PUKID = "subject_pukid";
    // public static final String  PUBLIC_KEY ="public_key";
    public static final String ISSUER_PUK = "issuer_puk";
    public static final String SUBJECT_PUK = "subject_puk";
    public static final String PRIMARY_SIG = "private_sig";
    public static final String MEK_SIG = "mek_sig";
    public static final String SERIAL_NUM = "serial_num";
    public static final String ISSUER_SIG = "issuer_sig";
    public static final String SUBJECT_SIG = "subject_sig";
    public static final String BCL = "bcl";
    public static final String BCL_BODY_VALIDITY = "validity";
    public static final String BCL_BODY_POLICIES = "policies";
    public static final String BRL = "brl";
    public static final String BRL_BODY_REVOKED = "revoked";
    public static final String BRL_PUKID = "brl_pukid";
    public static final String BRL_SIG = "brl_sig";

    //used by inplace encrypt
    public static final String KEYNAME_USER = "user";
    public static final String KEYNAME_DATABASE = "database";
    public static final String KEYNAME_SCHEMA = "schema";
    public static final String KEYNAME_TABLE = "table";
    public static final String KEYNAME_COLUMN = "column";
    public static final String RECURSIVE_SEARCH = "recursive_search";

    public static final String DEST_KEYNAME = "dest_keyname";
    public static final String SRC_KEYNAME = "src_keyname";

    public static final String ENC_RULE = "enc_rule";

    public static final String RULES = "rules";

    public static final String NAME = "name";

    public static final String ENABLED = "enabled";

    public static final String META = "meta";

    public static final String USERS = "users";

    public static final String RESTRICTED_ACCESS = "restrictedAccess";
    public static final String FULL_ACCESS = "fullAccess";

    public static final String DATABASES = "databases";

    public static final String TABLES = "tables";

    public static final String COLUMNS = "columns";

    public static final String DESCRIPTION = "description";

    public static final String STATUS = "status";

    public static final String BODY = "body";

    public static final int BCL_VERSION_1 = 1;
    public static final int BRL_VERSION_1 = 1;
}
