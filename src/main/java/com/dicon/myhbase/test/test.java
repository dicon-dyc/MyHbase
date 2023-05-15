package com.dicon.myhbase.test;


import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;

public class test {
//
//    @Test
//    public static void main(String[] args) {
//
//
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum","");
//        configuration.set("hbase.zookeeper.property.clientPort","2181");
//        try {
//            Connection connection = ConnectionFactory.createConnection(configuration);
//            Admin admin = connection.getAdmin();
//            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("apitest");
//            NamespaceDescriptor build = builder.build();
//            admin.createNamespace(build);
//            admin.close();
//
//        } catch (IOException e) {
//            throw new RuntimeException("连接失败");
//
//        }
//    }

//         * {
//     *     "tableName":"namespace:tablename"
//                *     ,"rowKey":"rowkey"
//                *     ,"columnsFamily":"columnsfamily"
//                *     ,"columns":["column 1","column 2"]
//     *     ,"values":["column 1  value","column 2 value"]
//     * }

    public static void main(String[] args){

        String upSertData = "{\n" +
                "    \"tableName\":\"dyc:testTable\"\n" +
                "    ,\"rowKey\":\"dyc01\"\n" +
                "    ,\"columnsFamily\":\"Column Family 1\"\n" +
                "    ,\"columns\":[\"name\",\"age\"]\n" +
                "    ,\"values\":[\"dyc\",\"1\"]\n" +
                "}";

        JSONObject jsonObject = JSONObject.parseObject(upSertData);

        String tableName = jsonObject.getString("tableName");
        System.out.println(tableName);

        String rowKey = jsonObject.getString("rowKey");
        System.out.println(rowKey);

        String columnFamily = jsonObject.getString("columnsFamily");
        System.out.println(columnFamily);

        String[] columns = jsonObject.getJSONArray("columns").toArray(new String[0]);
        System.out.println(Arrays.toString(columns));

        String[] values = jsonObject.getJSONArray("values").toArray(new String[0]);
        System.out.println(Arrays.toString(values));
    }

}
