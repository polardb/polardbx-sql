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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chenghui.lch
 */
public class DbNameUtil {

    private final static int MAX_DB_NAME_LENGTH = 32;

    private static Set<String> invalidKeyWords = new HashSet<>();

    private final static String invalidKeywords =
        "using,hour_second,float,references,over,containstable,current_user,right,else,tinytext,host,ha,day_microsecond,lines,zerofill,view,each,explain,separator,mysql,unique,before,net,no_write_to_binlog,leave,sqlexception,loop,current,write,left,f5,shutdown,ignore,linear,setuser,session_user,integer,setupadmin,keys,checkpoint,web,join,operator,cangjie,analyze,readtext,day_second,developer,exists,guest,having,dayu,netops,public,client,root,drop,change,dba,distributed,fillfactor,by,apache,long,close,insensitive,dbo,any,gongcao,key,updatetext,execute,infile,monitor,faq,label,double,and,day_minute,cm,current_date,column,freetext,webnet,galaxy,webmaster,update,serveradmin,set,distinctrow,sqlwarning,statistics,nonclustered,openxml,mssqlsystemresource,all,schemas,precision,offsets,as,cascade,off,nas,sqlengine,longblob,clustered,manager,enclosed,break,of,help,replication,on,identitycol,purge,xtrabak,limit,coalesce,fetch,services,support,or,distribution,pe,processadmin,localtimestamp,then,lvs,regexp,fuxi,binary,interval,constraint,inout,sqlonline,sysadmin,adminsys,tempdb,sql_big_result,null,backup,rowcount,spatial,bigint,true,netweb,master,dbcc,sql,force,insert,minute_second,alimail,opsdb,save,char,disk,contains,authorization,float8,pangu,where,float4,revoke,siteops,primary,when,utc_date,trailing,int8,int,int3,int4,int1,int2,zhongkui,restore,intersect,release,msdb,reconfigure,hr,mediumint,rollback,from,security,add,while,real,wangwang,information_schema,securityadmin,if,yunti,groupon,read,compute,outfile,between,fulltext,postmaster,appadmin,is,sql_calc_found_rows,into,x509,in,print,database,plan,sqlstate,raiserror,dataengine,schema,option,meituan,system_user,mod,yum,mediumblob,optionally,declare,reads,second_microsecond,system,lineno,leading,elseif,load,dummy,nuwa,asensitive,desc,sys,use,tinyint,errlvl,shennong,nullif,reply,procedure,varchar,sensitive,localtime,news,delayed,dns,raid0,bulk,alter,replace,rlike,admin,diskadmin,to,percent,both,inner,no-reply,openquery,escaped,collate,download,ssl,varcharacter,ntp,values,commit,browse,ssladmin,varying,login,index,nvwa,condition,select,opendatasource,optimize,waitfor,apsara,sa,freetexttable,exec,require,case,foreign,varbinary,natural,modifies,model,dump,repeat,iterate,textsize,deny,qq,top,xor,utc_timestamp,hostmaster,usage,smallint,out,mediumtext,numeric,cursor,for,test,longtext,info,distinct,youchao,open,minute_microsecond,file,sales,eagleye,describe,false,opr,kuafu,ops,unlock,national,high_priority,table,like,create,exit,not,san,asc,year_month,cdn,truncate,dbcreator,some,cross,range,goto,nocheck,escape,character,dual,identity,dec,union,delete,starting,deterministic,proc,end,trigger,post,utc_time,return,unsigned,gongming,current_timestamp,syslog,databases,terminated,hour_minute,taoyun,writetext,grant,transaction,tinyblob,undo,rename,kill,show,function,bulkadmin,holdlock,tsequal,straight_join,except,connection,middleint,openrowset,tran,restrict,default,rowguidcol,replicator,identity_insert,match,hour_microsecond,call,day_hour,specific,div,convert,oracle,aliyun,order,full,rule,with,check,aurora,sql_small_result,tianyun,lock,current_time,decimal,squid,begin,blob,outer,administrator,continue,deallocate,mssqld,houyi,group,user,low_priority,performance_schema";

    static {
        String[] keyWordArr = invalidKeywords.split(",");
        for (int i = 0; i < keyWordArr.length; i++) {
            invalidKeyWords.add(keyWordArr[i]);
        }
        invalidKeyWords.add(SystemDbHelper.DEFAULT_DB_NAME);
        invalidKeyWords.add(SystemDbHelper.CDC_DB_NAME);
    }

    public static boolean validateDbName(String dbName) {

        if (dbName.length() > MAX_DB_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the length of dbName[%s] is too long", dbName));
            //return false;
        }

        for (int i = 0; i < dbName.length(); i++) {
            if (!isWord(dbName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Failed to create database because the dbName[%s] contains some invalid characters",
                        dbName));
            }
        }

        if (invalidKeyWords.contains(dbName.toLowerCase())) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the string of dbName[%s] is a keyword", dbName));
        }

        return true;
    }

    /**
     * check a char if is "a-z、A-Z、_、0-9"
     *
     * @return true if check ok
     */
    public static boolean isWord(char c) {
        String regEx = "[\\w]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher("" + c);
        return m.matches();
    }

    public static boolean isInvalidKeyWorkds(String words) {
        return invalidKeywords.contains(words);
    }

}
