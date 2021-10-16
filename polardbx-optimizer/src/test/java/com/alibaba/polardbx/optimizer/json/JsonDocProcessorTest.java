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

package com.alibaba.polardbx.optimizer.json;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * @author arnkore 2017-07-14 11:05
 */
public class JsonDocProcessorTest {
    /**
     * {
     * "id": "109548",
     * "name": "arnkore",
     * "age": "28",
     * "language": ["JAVA", "GO", "PYTHON"],
     * "tool":[{"IDE": "IDEA"}, {"editor": "VIM"}]
     * }
     */
    private static final String jsonDoc1 =
        "{\"id\": \"109548\",\"name\": \"arnkore\",\"age\": \"28\",\"language\": [\"JAVA\", \"GO\", \"PYTHON\"],\"tool\":[{\"IDE\": \"IDEA\"}, {\"editor\": \"VIM\"}]}";

    /**
     * [{"IDE": "IntellJIdea", "editor": "vim"}, {"IDE": "eclipse", "editor": "emacs"}]
     */
    private static final String jsonDoc2 =
        "[{\"IDE\": \"IntellJIdea\", \"editor\": \"vim\"}, {\"IDE\": \"eclipse\", \"editor\": \"emacs\"}]";

    /**
     * {
     * "a": 1,
     * "b": [2, 3]
     * }
     */
    private static final String jsonDoc3 = "{\"a\": 1, \"b\": [2, 3]}";

    @Test
    public void testJsonExtractForJsonObject() throws SQLSyntaxErrorException {
        Assert.assertEquals("109548", JsonUtil.unquote(jsonOrValueString(jsonDoc1, "$.id")));
        Assert.assertEquals("JAVA", JsonUtil.unquote(jsonOrValueString(jsonDoc1, "$.language[0]")));
        Assert.assertEquals("VIM", JsonUtil.unquote(jsonOrValueString(jsonDoc1, "$.tool[1].editor")));

        String[] languages = new String[] {"JAVA", "GO", "PYTHON"};
        Assert.assertEquals(JSON.toJSONString(languages), jsonOrValueString(jsonDoc1, "$.language"));

        Assert.assertNull(extractJsonOrValue(jsonDoc1, "$.title"));
        Assert.assertNull(extractJsonOrValue(jsonDoc1, "$.id.hehe"));
        Assert.assertNull(extractJsonOrValue(jsonDoc1, "$.language[0].hehe"));
        Assert.assertNull(extractJsonOrValue(jsonDoc1, "$.tool[1].editor.hehe"));
    }

    @Test
    public void testJsonExtractForJsonArray() throws SQLSyntaxErrorException {
        Assert.assertEquals("IntellJIdea", JsonUtil.unquote(jsonOrValueString(jsonDoc2, "$[0].IDE")));
        String[] tools = new String[] {"vim", "IntellJIdea"};
        Assert.assertEquals(JSON.toJSONString(tools), JsonUtil.unquote(jsonOrValueString(jsonDoc2, "$[0].*")));
    }

    private static String jsonOrValueString(String jsonDoc, String pathExpr) throws SQLSyntaxErrorException {
        return JSON.toJSONString(extractJsonOrValue(jsonDoc, pathExpr));
    }

    private static Object extractJsonOrValue(String jsonDoc, String pathExpr) throws SQLSyntaxErrorException {
        MySQLLexer lexer = new MySQLLexer(pathExpr);
        JsonPathExprParser parser = new JsonPathExprParser(lexer);
        JsonPathExprStatement jsonPathExpr = parser.parse();
        return JsonDocProcessor.extract(jsonDoc, jsonPathExpr);
    }

    private static JsonPathExprStatement parsePathExpr(String pathExpr) throws SQLSyntaxErrorException {
        MySQLLexer lexer = new MySQLLexer(pathExpr);
        JsonPathExprParser parser = new JsonPathExprParser(lexer);
        return parser.parse();
    }

    private static List<Pair<JsonPathExprStatement, Object>> generatePathValPairs(Object... params)
        throws SQLSyntaxErrorException {
        if (params.length % 2 != 0) {
            throw new IllegalArgumentException("Must specify even number arguments");
        }

        List<Pair<JsonPathExprStatement, Object>> resultPairs = Lists.newArrayList();
        JsonPathExprStatement stmt = null;
        Object val = null;
        for (int i = 0; i < params.length; i++) {
            if (i % 2 == 0) {
                stmt = parsePathExpr((String) params[i]);
            } else {
                val = params[i];
                resultPairs.add(new Pair<JsonPathExprStatement, Object>(stmt, val));
            }
        }

        return resultPairs;
    }

    private static List<JsonPathExprStatement> generatePathList(Object... params) throws SQLSyntaxErrorException {
        List<JsonPathExprStatement> jsonPathExprs = Lists.newArrayList();
        for (int i = 0; i < params.length; i++) {
            jsonPathExprs.add(parsePathExpr((String) params[i]));
        }
        return jsonPathExprs;
    }

    @Test
    public void testJsonInsertForJsonObject() throws SQLSyntaxErrorException {
        Object res = JsonDocProcessor.insert(jsonDoc3, generatePathValPairs("$.a", 10, "$.c", "[true, false]"));
        Assert.assertEquals("{\"a\":1,\"b\":[2,3],\"c\":\"[true, false]\"}", JSON.toJSONString(res));

        res = JsonDocProcessor.insert(jsonDoc3, generatePathValPairs("$.c", "[true, false]", "$[1]", 10));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3],\"c\":\"[true, false]\"},10]", JSON.toJSONString(res));

        res = JsonDocProcessor.insert(jsonDoc3, generatePathValPairs("$.a", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("{\"a\":1,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.insert(jsonDoc3, generatePathValPairs("$[1]", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3]},10]", JSON.toJSONString(res));

        res = JsonDocProcessor.insert(jsonDoc3, generatePathValPairs("$.c", "hehe", "$[2]", 10));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3],\"c\":\"hehe\"},10]", JSON.toJSONString(res));
    }

    @Test
    public void testJsonReplaceForJsonObject() throws SQLSyntaxErrorException {
        Object res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$.a", 10, "$.c", "[true, false]"));
        Assert.assertEquals("{\"a\":10,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$.c", "[true, false]", "$[1]", 10));
        Assert.assertEquals("{\"a\":1,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$.a", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("{\"a\":10,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$[1]", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("{\"a\":1,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$.c", "hehe", "$[2]", 10));
        Assert.assertEquals("{\"a\":1,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.replace(jsonDoc3, generatePathValPairs("$.b[0]", 1, "$.a", 10));
        Assert.assertEquals("{\"a\":10,\"b\":[1,3]}", JSON.toJSONString(res));
    }

    @Test
    public void testJsonSetForJsonObject() throws SQLSyntaxErrorException {
        Object res = JsonDocProcessor.set(jsonDoc3, generatePathValPairs("$.a", 10, "$.c", "[true, false]"));
        Assert.assertEquals("{\"a\":10,\"b\":[2,3],\"c\":\"[true, false]\"}", JSON.toJSONString(res));

        res = JsonDocProcessor.set(jsonDoc3, generatePathValPairs("$.c", "[true, false]", "$[1]", 10));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3],\"c\":\"[true, false]\"},10]", JSON.toJSONString(res));

        res = JsonDocProcessor.set(jsonDoc3, generatePathValPairs("$.a", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("{\"a\":10,\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.set(jsonDoc3, generatePathValPairs("$[1]", 10, "$.a.c", "[true, false]"));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3]},10]", JSON.toJSONString(res));

        res = JsonDocProcessor.set(jsonDoc3, generatePathValPairs("$.c", "hehe", "$[2]", 10));
        Assert.assertEquals("[{\"a\":1,\"b\":[2,3],\"c\":\"hehe\"},10]", JSON.toJSONString(res));
    }

    @Test
    public void testJsonRemoveForJsonObject() throws SQLSyntaxErrorException {
        Object res = JsonDocProcessor.remove(jsonDoc3, generatePathList("$.a", "$.c"));
        Assert.assertEquals("{\"b\":[2,3]}", JSON.toJSONString(res));

        res = JsonDocProcessor.remove(jsonDoc3, generatePathList("$.b[1]", "$.a.c"));
        Assert.assertEquals("{\"a\":1,\"b\":[2]}", JSON.toJSONString(res));

        res = JsonDocProcessor.remove(jsonDoc3, generatePathList("$.b[1]", "$.a"));
        Assert.assertEquals("{\"b\":[2]}", JSON.toJSONString(res));
    }

    @Test
    public void testJsonMerge() {
        Assert.assertEquals("[1,2,true,false]", JsonDocProcessor.jsonMerge(new String[] {"[1, 2]", "[true, false]"}));
        Assert.assertEquals("{\"name\":\"x\",\"id\":47}",
            JsonDocProcessor.jsonMerge(new String[] {"{\"name\": \"x\"}", "{\"id\": 47}"}));
        Assert.assertEquals("{\"name\":[\"x\",47]}",
            JsonDocProcessor.jsonMerge(new String[] {"{\"name\": \"x\"}", "{\"name\": 47}"}));
        Assert.assertEquals("{\"name\":[\"x\",47]}",
            JsonDocProcessor.jsonMerge(new String[] {"{\"name\": \"x\"}", "{\"name\": [47]}"}));
        Assert.assertEquals("{\"name\":[\"x\",47]}",
            JsonDocProcessor.jsonMerge(new String[] {"{\"name\": [\"x\"]}", "{\"name\": 47}"}));
        Assert.assertEquals("[1,true]", JsonDocProcessor.jsonMerge(new String[] {"1", "true"}));
        Assert.assertEquals("[1,2,{\"id\":47}]", JsonDocProcessor.jsonMerge(new String[] {"[1, 2]", "{\"id\": 47}"}));
        Assert.assertEquals("{\"a\":[1,2],\"b\":2}",
            JsonDocProcessor.jsonMerge(new String[] {"{\"a\":1}", "{\"b\":2}", "{\"a\":2}"}));
    }

    @Test
    public void testJsonKeys() throws SQLSyntaxErrorException {
        Assert.assertEquals("[\"a\",\"b\"]", jsonKeys("{\"a\": 1, \"b\": {\"c\": 30}}"));
        Assert.assertEquals("[\"c\"]", jsonKeys("{\"a\": 1, \"b\": {\"c\": 30}}", "$.b"));
        Assert.assertEquals("NULL", jsonKeys("{\"a\": 1, \"b\": {\"c\": 30}}", "$.c"));
        Assert.assertEquals("[]", jsonKeys("{\"a\": {}, \"b\": {\"c\": 30}}", "$.a"));
    }

    private String jsonKeys(String jsonDoc) throws SQLSyntaxErrorException {
        return jsonKeys(jsonDoc, null);
    }

    private String jsonKeys(String jsonDoc, String pathExpr) throws SQLSyntaxErrorException {
        Object jsonObj = JsonUtil.parse(jsonDoc);
        if (pathExpr != null) {
            JsonPathExprStatement jsonPathStmt = parsePathExpr(pathExpr);
            return JsonDocProcessor.jsonKeys(jsonObj, jsonPathStmt);
        } else {
            return JsonDocProcessor.jsonKeys(jsonObj, null);
        }
    }
}
