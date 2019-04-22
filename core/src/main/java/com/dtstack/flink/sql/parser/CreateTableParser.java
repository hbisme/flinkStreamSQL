/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flink.sql.parser;

import com.dtstack.flink.sql.util.DtStringUtil;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析创建表结构sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class CreateTableParser implements IParser {

    /* "create table myTable (name varchar, age int)  with (type = 'kafka09', topic = 'nbTest1')"  解析这样的sql
        tableName: myTable
        fieldsInfoStr: name varchar, age int
        propsStr: "type = 'kafka09', topic = 'nbTest1'"
        props: Map{topic=hbTest1, type=kafka09}
     */
    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateTableParser newInstance() {
        return new CreateTableParser();
    }

    @Override
    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if (matcher.find()) {
            String tableName = matcher.group(1).toUpperCase();
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, Object> props = parseProp(propsStr);

            /*
            * 生成sql解析结果类(静态内部类)
            * */
            SqlParserResult result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            /*
            * addPreDealTableInfo 的作用是 将sqlTree 的 preDealTableMap 变量增加一项:
            * key 是: myTable (tableName), value 是: sql解析结果类
            *
            * */
            sqlTree.addPreDealTableInfo(tableName, result);
        }
    }

    private Map parseProp(String propsStr) {
        String[] strs = propsStr.trim().split("'\\s*,");
        Map<String, Object> propMap = Maps.newHashMap();
        for (int i = 0; i < strs.length; i++) {
            List<String> ss = DtStringUtil.splitIgnoreQuota(strs[i], '=');
            String key = ss.get(0).trim();
            String value = ss.get(1).trim().replaceAll("'", "").trim();
            propMap.put(key, value);
        }

        /* 将propsStr(属性字符串)转成HashMap
         * */
        return propMap;
    }

    /*
    * sql解析结果内部类,包含三个字段:
    * 1. tableName 比如: "myTable"
    * 2. fieldsInfoStr 比如: "name varchar, age int"
    * 3. propMap 比如 Map{topic=hbTest1, type=kafka09}
    *
    * */
    public static class SqlParserResult {

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, Object> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, Object> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, Object> propMap) {
            this.propMap = propMap;
        }
    }


    /*
     * 测试 parseSql (解析sql),也可用于debug
     *
     *
     * */
    public static void main(String[] args) {

        CreateTableParser createTableParser = new CreateTableParser();
        String createSql = "create table myTable (name varchar,age int)  with (type = 'kafka09', topic = 'nbTest1')";
        SqlTree sqlTree = new SqlTree();
        System.out.println(sqlTree);
        createTableParser.parseSql(createSql, sqlTree);
        System.out.println(sqlTree);

    }
}
