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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析创建自定义方法sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CreateFuncParser implements IParser {

    /*
    * 解析这样的sql: " create scala  function myFunc  with com.hb.myClass "
    *   type: scala
    *   funcName: myFunc
    *   className: com.hb.myClass
    * */
    private static final String funcPatternStr = "(?i)\\s*create\\s+(scala|table)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern funcPattern = Pattern.compile(funcPatternStr);

    @Override
    public boolean verify(String sql) {
        return funcPattern.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = funcPattern.matcher(sql);
        if(matcher.find()){
            String type = matcher.group(1);
            String funcName = matcher.group(2);
            String className = matcher.group(3);
            SqlParserResult result = new SqlParserResult();
            result.setType(type);
            result.setName(funcName);
            result.setClassName(className);
            /*
            *  将sql解析后的结果,添加到sqlTree的 List<CreateFuncParser.SqlParserResult> functionList 中
            *
            * */
            sqlTree.addFunc(result);
        }
    }


    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }

    /*
    * sql解析结果的内部类,字段有:
    * 1. name: (比如: myFunc)
    * 2. className: (比如: com.hb.myClass)
    * 3. type(比如: scala)
    *
    * */
    public static class SqlParserResult{

        private String name;

        private String className;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    /*
    * 测试 parseSql (解析func),也可用于debug
    *
    *
    * */
    public static void main(String[] args) {
        String sql = " create scala  function myFunc  with com.hb.myClass ";
        SqlTree sqlTree = new SqlTree();
        CreateFuncParser createFuncParser = CreateFuncParser.newInstance();
        /* 将数据写到sqlTree  */
        createFuncParser.parseSql(sql, sqlTree);

    }


}
