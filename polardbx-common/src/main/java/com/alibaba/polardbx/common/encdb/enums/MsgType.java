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

public final class MsgType {
    // Server info
    public static final int SERVER_INFO_GET = 0x00;    //request server info
    public static final int SERVER_GET_NONCE = 0x01; // request a new server nonce

    // Remote attestation
    public static final int RA_CHALLENGE = 0x10;
    public static final int RA_01 = 0x11;                  //always combine message 0 and message 1
    public static final int RA_2 = 0x12;
    public static final int RA_3 = 0x13;
    public static final int RA_4 = 0x14;
    public static final int RA_GET_PUBLIC_KEY = 0x15;
    public static final int RA_GET_QUOTE = 0x16;          //generate RA quote
    public static final int RA_VERIFY_QUOTE = 0x17;

    // MEK
    public static final int MEK_PROVISION = 0x30;
    public static final int MEK_UPDATE = 0x31;
    public static final int MEK_IMPORT_FROM_KMS = 0x32;   //import MEK in KMS format
    public static final int MEK_EXPORT = 0x33;

    // DEK
    public static final int DEK_GENERATE = 0x50;       //generate in enclave
    public static final int DEK_INSERT = 0x51;        //insert DEK from local, should be encrypted in specified format
    public static final int DEK_UPDATE = 0x52;             //update DEK info
    public static final int DEK_GET_BY_NAME = 0x53;        //get DEK by keyname
    public static final int DEK_GET_BY_ID = 0x54;     //get DEK by ctxid
    public static final int DEK_GET_NEXT_DEK_ID = 0x55; //allocate a new dek entry and get latest dekid
    public static final int DEK_UPDATE_ATTRIBUTE = 0x56;        //update DEK CC attributes info
    public static final int DEK_UPDATE_ALGORITHM = 0x57;    //update DEK CC algorithm info
    public static final int DEK_COPY_KEYNAME = 0x58;
    public static final int KEYNAME_GENERATE = 0x59;
    public static final int KEYNAME_SEARCH = 0x5A;

    // BCL
    public static final int BCL_ISSUE = 0x70;               // issue or update BCL
    public static final int BCL_GET = 0x71;               // get BCL, including related BRL
    public static final int BCL_REVOKE = 0x72;              // issue BRL to revoke BCL
    public static final int BCL_REGISTER = 0x73;              // register user public key

    // keyname

    // EnclaveRequestType
    public static final int ENCLAVE_EXPORT_MEK = 0x90;
    public static final int ENCLAVE_GEN_KEY_PAIR = 0x91;

    public static final int ENC_RULE_IMPORT = 0xA0;

    public static final int ENC_RULE_DELETE = 0xA1;
}
