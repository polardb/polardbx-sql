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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.fastjson.JSONObject;

/**
 * The session variables during creating partitioned tables
 * 
 * @author chenghui.lch
 */
public class PartInfoSessionVars {
    
    public static final String EXTRA_JSON_KEY = "sessionVars";
    
    protected String timeZone = "";
    protected String charset = "";
    protected String collation = "";

    public PartInfoSessionVars() {
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }


    public static PartInfoSessionVars fromJsonObj(JSONObject sessionVarsJson) {
        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
        sessionVars.setTimeZone(sessionVarsJson.getString("timeZone"));
        sessionVars.setCharset(sessionVarsJson.getString("charset"));
        sessionVars.setCollation(sessionVarsJson.getString("collation"));
        return sessionVars;
    }
    
    public static JSONObject toJsonObj(PartInfoSessionVars sessionVars) {
        JSONObject sessVarsJsonObj = new JSONObject();
        sessVarsJsonObj.put("timeZone", sessionVars.timeZone);
        sessVarsJsonObj.put("charset", sessionVars.charset);
        sessVarsJsonObj.put("collation", sessionVars.collation);        
        return sessVarsJsonObj;
    }
    
    public PartInfoSessionVars copy() {
        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
        sessionVars.setCharset(this.charset);
        sessionVars.setTimeZone(this.timeZone);
        sessionVars.setCollation(this.collation);
        return sessionVars;
    }
}
