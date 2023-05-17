# Hbase Connector
目的是实现connector（与wfy-belief合作实现）下的HbaseConnector，当前版本为0.0.1（实现基本功能为主，鲁棒性较差）2023/05/17。 <br>
下一版本目标：待Connector初步抽象后，HbaseConnector对Connector进行适配。

## 使用
1、开放端口为:8088，可自行在application-test.yml配置。<br>
2、Hbase相关信息可自行在application-test.yml中配置,如配置路径有变更请在配置类 com.dicon.myhbase.config.HBaseConfig中同步更新。<br>

## 已实现功能
### 1、创建namespace
接口：8088/HBase/createNameSpace?nameSpace=参数 <br>
参数：GET nameSpace <br>
返回值：null 

### 2、创建table
接口：8088/HBase/createTable <br>
参数：
{
"tableName":"dyc:testTable"
,"families":["Column Family 1","Column Family 2"]
} <br>
返回值：null

### 3、插入、更新数据
接口：8088/HBase/upsertData <br>
参数：{
"tableName":"dyc:testTable"
,"rowKey":"dyc01"
,"columnsFamily":"Column Family 1"
,"columns":["name","age"]
,"values":["dyc","1"]
} <br>
返回值：null

### 4、通过rowkey获取数据
接口：8088/HBase/getDataByRowkey <br>
参数：{
"tableName":"dyc:testTable"
,"rowKey":"dyc01"
} <br>
返回值:List<HbasePojo>

### 5、扫描数据
接口：8088/HBase/scanData <br>
参数：{
"tableName":"dyc:testTable"
,"startRowKey":"Column Family 1:col1"
,"endRowKey":"dyc02"
} <br>
返回值：List<HbasePojo>

### 6、删除表
接口：8088/HBase/deleteTable <br>
参数：{
"tableName":"wfy:StudentAndCourse"
} <br>
返回值：null

### 7、删除rowkey
接口：8088/HBase/deleteRowKey <br>
参数：{
"tableName":"wfy:StudentAndCourse"
,"rowKey":"2015001"
} <br>
返回值：null

### 8、删除列族
接口：8088/HBase/deleteColumnsFamily <br>
参数：{
"tableName":"wfy:StudentAndCourse"
,"rowKey":"2015001"
,"columnsFamily":"student"
} <br>
返回值：null

### 9、删除列
接口：8088/HBase/deleteColumn <br>
参数：{
"tableName":"wfy:StudentAndCourse"
,"rowKey":"2015001"
,"columnsFamily":"student"
,"column":"S_age"   
} <br>
返回值：

## 待实现功能


