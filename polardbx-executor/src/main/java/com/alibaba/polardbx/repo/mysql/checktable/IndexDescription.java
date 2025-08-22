package com.alibaba.polardbx.repo.mysql.checktable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndexDescription {


    public static Pattern gppPattern = Pattern.compile("IFT\\s*=\\s*(\\d+)");

    public static Boolean parseGppOption(Boolean withGppEnabledColumn, String option){
        option = option.toUpperCase();
        Boolean flag = false;
        if(withGppEnabledColumn){
            return (Integer.parseInt(option) > 0);
        }
        String[] options = option.split(";");
        for(String singleOption : options){
            Matcher matcher = gppPattern.matcher(singleOption);
            if(matcher.find()){
                long value = Long.parseLong(matcher.group(1));
                flag = value > 0L;
            }
        }
        return flag;
    }


    public IndexDescription(String physicalDbName, String physicalTableName, String indexName) {
        this.physicalDbName = physicalDbName;
        this.physicalTableName = physicalTableName;
        this.indexName = indexName;
        this.columns = new LinkedHashMap<>();
        this.gpp = false;
    }

    String physicalDbName;
    String physicalTableName;
    String indexName;
    Boolean gpp;

    public void setGpp(Boolean gpp) {
        this.gpp = gpp;
    }

    public Boolean getGpp() {
        return gpp;
    }

    public Map<Integer, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<Integer, String> columns) {
        this.columns = columns;
    }

    Map<Integer, String> columns;

    public void appendColumn(Integer index, String column) {
        this.columns.put(index, column);
    }
}
