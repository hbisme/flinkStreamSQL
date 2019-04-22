// package com.dtstack.flink.sql.parser;
//
// import com.dtstack.flink.sql.util.DtStringUtil;
//
// import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
//
// import java.util.List;
// import java.util.Map;
// import java.util.regex.Matcher;
// import java.util.regex.Pattern;
//
// /**
//  * 解析创建表结构sql
//  * Date: 2018/6/26
//  * Company: www.dtstack.com
//  *
//  * @author xuchao
//  */
//
// public class Tmp1 implements IParser {
//
//     /* "create table myTable (name varchar,channel varchar)  with (key1=value1, key2=value2 )"  解析这样的sql
//         tableName: myTable
//         fieldsInfoStr: field1,field2
//         propsStr: key1=value1 key2=value2
//      */
//     private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";
//
//     private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);
//
//     public static CreateTableParser newInstance() {
//         return new CreateTableParser();
//     }
//
//     @Override
//     public boolean verify(String sql) {
//         return PATTERN.matcher(sql).find();
//     }
//
//     @Override
//     public void parseSql(String sql, SqlTree sqlTree) {
//         Matcher matcher = PATTERN.matcher(sql);
//         if (matcher.find()) {
//             String tableName = matcher.group(1).toUpperCase();
//             String fieldsInfoStr = matcher.group(2);
//             String propsStr = matcher.group(3);
//             Map<String, Object> props = parseProp(propsStr);
//
//             System.out.println();
//             CreateTableParser.SqlParserResult result2 = new CreateTableParser.SqlParserResult();
//             result2.setTableName(tableName);
//             result2.setFieldsInfoStr(fieldsInfoStr);
//             result2.setPropMap(props);
//
//             sqlTree.addPreDealTableInfo(tableName, result2);
//         }
//     }
//
//     private Map parseProp(String propsStr) {
//         String[] strs = propsStr.trim().split("'\\s*,");
//         Map<String, Object> propMap = Maps.newHashMap();
//         for (int i = 0; i < strs.length; i++) {
//             List<String> ss = DtStringUtil.splitIgnoreQuota(strs[i], '=');
//             String key = ss.get(0).trim();
//             System.out.println();
//             String value = ss.get(1).trim().replaceAll("'", "").trim();
//             propMap.put(key, value);
//         }
//
//         return propMap;
//     }
//
//     // public static class SqlParserResult {
//     //
//     //     private String tableName;
//     //
//     //     private String fieldsInfoStr;
//     //
//     //     private Map<String, Object> propMap;
//     //
//     //     public String getTableName() {
//     //         return tableName;
//     //     }
//     //
//     //     public void setTableName(String tableName) {
//     //         this.tableName = tableName;
//     //     }
//     //
//     //     public String getFieldsInfoStr() {
//     //         return fieldsInfoStr;
//     //     }
//     //
//     //     public void setFieldsInfoStr(String fieldsInfoStr) {
//     //         this.fieldsInfoStr = fieldsInfoStr;
//     //     }
//     //
//     //     public Map<String, Object> getPropMap() {
//     //         return propMap;
//     //     }
//     //
//     //     public void setPropMap(Map<String, Object> propMap) {
//     //         this.propMap = propMap;
//     //     }
//     // }
//
//     public static void main(String[] args) {
//         System.out.println("start");
//         Tmp1 tmp1 = new Tmp1();
//         String str = "create table myTable (name varchar,channel varchar)  with (key1=value1 key2=value2 )";
//         SqlTree sqlTree = new SqlTree();
//         System.out.println(sqlTree);
//         tmp1.parseSql(str, sqlTree);
//         System.out.println(sqlTree);
//
//     }
// }