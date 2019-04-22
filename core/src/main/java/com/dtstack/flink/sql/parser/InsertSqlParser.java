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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * 解析flink sql
 * sql 语句如:
 *         INSERT INTO `MYRESULT`
 *             (SELECT `D`.`CHANNEL`, `D`.`INFO`
 *             FROM
 *             (SELECT `A`.*, `B`.`INFO`
 *                 FROM `MYTABLE` AS `A`
 *             INNER JOIN `SIDETABLE` AS `B` ON `A`.`CHANNEL` = `B`.`NAME`
 *             WHERE `A`.`CHANNEL` = 'xc2' AND `A`.`PV` = 10) AS `D`)
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class InsertSqlParser implements IParser {

    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance(){
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        /*
        * 生成sql解析器
        * */
        SqlParser sqlParser = SqlParser.create(sql);
        SqlNode sqlNode = null;
        try {
            // sqlNode是Tree形式的SQL Tree
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }

        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);
        sqlParseResult.setExecSql(sqlNode.toString());

        /*
        * sqlTree中,加入可执行sql语句的解析结果
        * */
        sqlTree.addExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                sqlParseResult.addTargetTable(sqlTarget.toString());
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                if(sqlFrom.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                }else{
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();

                if(leftNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else{
                    parseNode(leftNode, sqlParseResult);
                }

                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                if(identifierNode.getKind() != IDENTIFIER){
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            default:
                //do nothing
                break;
        }
    }

    /*
    * sql解析结果,包含
    * 1.sourceTableList: 源表名称的列表, 如: List("MYTABLE", "SIDETABLE")
    * 2.targetTableList: 目标表名称的列表, 如: LIST("MYRESULT")
    * 3.execSql 执行的sql语句,如:
    *   INSERT INTO `MYRESULT`
            (SELECT `D`.`CHANNEL`, `D`.`INFO`
            FROM
            (SELECT `A`.*, `B`.`INFO`
                FROM `MYTABLE` AS `A`
            INNER JOIN `SIDETABLE` AS `B` ON `A`.`CHANNEL` = `B`.`NAME`
            WHERE `A`.`CHANNEL` = 'xc2' AND `A`.`PV` = 10) AS `D`)
    * */
    public static class SqlParseResult {

        private List<String> sourceTableList = Lists.newArrayList();

        private List<String> targetTableList = Lists.newArrayList();

        private String execSql;

        public void addSourceTable(String sourceTable){
            sourceTableList.add(sourceTable);
        }

        public void addTargetTable(String targetTable){
            targetTableList.add(targetTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }

        public List<String> getTargetTableList() {
            return targetTableList;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }
    }


    /*
    * 测试 parseSql (解析sql),也可用于debug
    *
    * */
    public static void main(String[] args) {
        String sql =
                " insert into MyResult\n" +
                "    select\n" +
                "        d.channel,\n" +
                "        d.info\n" +
                "    from(\n" +
                "    select\n" +
                "            a.*,b.info\n" +
                "        from MyTable a\n" +
                "        join sideTable b\n" +
                "        on a.channel = b.name\n" +
                "        where\n" +
                "            a.channel = 'xc2'\n" +
                "            and a.pv=10      \n" +
                "    ) as d";
        SqlTree sqlTree = new SqlTree();
        InsertSqlParser insertSqlParser = InsertSqlParser.newInstance();
        /* 将数据写到sqlTree  */
        insertSqlParser.parseSql(sql, sqlTree);
        System.out.println(sqlTree);

    }

}
