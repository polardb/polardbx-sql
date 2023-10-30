package com.alibaba.polardbx.qatest.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.statistic.StatisticCostModelTest;
import com.google.common.collect.Maps;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.stream.IntStream;

public class DiffRepository {
    public static Double[] getDoubleArray(String filePath, String key) {
        Yaml yaml = new Yaml();
        Map<String, String> yamlMap =
            yaml.loadAs(DiffRepository.class.getResourceAsStream(filePath), Map.class);
        if (yamlMap == null) {
            return null;
        }
        String val = yamlMap.get(key);
        if (StringUtils.isEmpty(val)) {
            return null;
        }
        JSONArray jsonArray = JSON.parseArray(val);
        Double[] rs = new Double[jsonArray.size()];
        IntStream.range(0, rs.length).forEach(i -> rs[i] = jsonArray.getDouble(i));
        return rs;
    }

    public static void writeDoubleArray(String filePath, String key, Double[] upLimit) throws IOException {
        Yaml yaml = new Yaml();
        Map<String, String> yamlMap =
            yaml.loadAs(DiffRepository.class.getResourceAsStream(filePath), Map.class);
        if (yamlMap == null) {
            yamlMap = Maps.newConcurrentMap();
        }
        yamlMap.put(key, JSON.toJSONString(upLimit));
        FileWriter writer = null;
        BufferedWriter bufferedWriter = null;
        try {
            writer = new FileWriter(DiffRepository.class.getResource(filePath).getFile());
            bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(yaml.dump(yamlMap));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bufferedWriter.flush();
            bufferedWriter.close();
            writer.close();
        }
    }
}

// End DiffRepository.java
