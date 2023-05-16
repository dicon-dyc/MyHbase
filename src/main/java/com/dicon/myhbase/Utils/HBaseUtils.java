package com.dicon.myhbase.Utils;

import com.dicon.myhbase.Pojo.HbasePojo;
import com.dicon.myhbase.config.HBaseConfig;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author:dyc
 * @Date:2023/05/12
 */
@Component
public class HBaseUtils {

    private final Logger log =  LogManager.getLogger(com.dicon.myhbase.Utils.HBaseUtils.class);

    private Connection connection = null;

    private Admin admin = null;

    private Configuration configuration = null;


    /**
     * 有参构造，交给spring容器管理
     * @param hbaseConfig hbase连接的config信息
     */
    private HBaseUtils(HBaseConfig hbaseConfig){

        try {
//            Configuration configuration = HBaseConfiguration.create();
            this.configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",hbaseConfig.getQuorum());
            configuration.set("hbase.zookeeper.property.clientPort", hbaseConfig.getClientPort());

            this.connection = ConnectionFactory.createConnection(configuration);
            this.admin = connection.getAdmin();
            log.info("---------------------admin" + admin);
        } catch (IOException e) {
            log.error("获取hbase连接失败:{}",e.getMessage(),e);
        }

    }


    /**
     * 创建nameSpace
     *
     * @param nameSpace 需要创建的namespace
     * @throws IOException 连接hbase失败
     */
    public void createNamespace(String nameSpace) throws IOException {

        admin = connection.getAdmin();

        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);

        NamespaceDescriptor build = builder.build();

        try {
            admin.createNamespace(build);
        } catch (IOException e) {

            log.error("createNamespace创建失败："+nameSpace);
            throw new RuntimeException(e);
        }


    }


    /**
     * 判断表是否存在
     *
     * @param tableName 需要判断的表名
     * @throws IOException 连接hbase失败
     * @return boolean 存在返回true，不存在返回false
     */
    public boolean tableExists(String tableName) throws IOException {

        TableName[] tableNames = admin.listTableNames();

        for (TableName name : tableNames) {
            if (name.equals(tableName)){
                return false;
            }
        }
        return true;

    }


    /**
     * 创建table
     *
     * @param tableName 要创建的表名 like namespace:tablename
     * @param columnFamilies 要创建的列族集合
     */
    public void createTable(String tableName, List<String> columnFamilies) throws IOException {


        TableName name = TableName.valueOf(tableName);

        //判断表是否存在
        if (this.tableExists(tableName)){

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(name);

            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();

            for (String columnFamily : columnFamilies) {

                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(columnFamily.getBytes()).build();
                columnFamilyDescriptorList.add(columnFamilyDescriptor);
            }

            tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptorList);

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

            admin.createTable(tableDescriptor);

            log.info("创建成功");



        }

    }


    /**
     * 插入或更新单个数据,无论单个数据或是多个数据都可以通过插入或更新多个数据方法来时间。
     *
     * @param tableName 要插入或更新数据的表名 "namespace:tablename"
     * @param rowKey 要插入或更新的rowkey
     * @param columnFamily 要插入或更新的列族
     * @param column 要插入或更新的列
     * @param value 要插入或更新的值
     */
    @Deprecated
    public void upsertData(String tableName,String rowKey,String columnFamily,String column,String value){

        this.upsertData(tableName,rowKey,columnFamily,new String[]{column},new String[]{value});
    }


    /**
     * 插入或更新多个数据
     *
     * @param tableName 要插入或更新数据的表名 "namespace:tablename"
     * @param rowKey 要插入或更新数据的rowkey
     * @param columnFamily 要插入或更新数据的列族
     * @param columns 要插入或更新数据的列集合
     * @param values 要插入或更新数据的值集合
     */
    public void upsertData(String tableName,String rowKey,String columnFamily,String[] columns,String[] values){

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));

            for (int i = 0; i < columns.length; i++) {

                log.info("插入或更新数据：columns:"+columns[i]+"values:"+values[i]);

                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
                table.put(put);
            }

            table.close();

        } catch (IOException e) {

            log.info("upsertData:connection.getTable 获取连接失败");
            throw new RuntimeException(e);
        }


    }

    /**
     * 查询指定rowkey的值
     *
     * @param tableName 需要查询的表名:"namespace:tablename"
     * @param rowKey 需要查询的rowkey
     * @return List<HbasePojo> 返回结果列表
     */
    public List<HbasePojo> getDataByRowKey(String tableName,String rowKey){

        try {

            Table table = connection.getTable(TableName.valueOf(tableName));

            Get get = new Get(rowKey.getBytes());

            Result result = table.get(get);

            List<HbasePojo> resultList = new ArrayList<>();


            for (Cell cell : result.rawCells()){

                HbasePojo hbasePojo = new HbasePojo();

                hbasePojo.setTableName(tableName);

                hbasePojo.setRowKey(rowKey);

                //CellUtil解决乱码问题
                hbasePojo.setColumnsFamily(Bytes.toString(CellUtil.cloneFamily(cell)));

                hbasePojo.setColumns(Bytes.toString(CellUtil.cloneQualifier(cell)));

                hbasePojo.setValues(Bytes.toString(CellUtil.cloneValue(cell)));

                resultList.add(hbasePojo);

            }
            table.close();

            return resultList;
            
        } catch (IOException e) {
            log.info("getData:connection.getTable 获取连接失败");
            throw new RuntimeException(e);
        }

    }


    /**
     * 扫描获取数据
     *
     * @param tableName 要扫描的表 "namespace:tablename"
     * @param startRowKey 起始rowkey
     * @param endRowKey 结束rowkey(不包含结束rowkey)
     * @return 结果集合
     * @throws IOException io异常，连接表失败
     */
    public List<HbasePojo> scanData(String tableName,String startRowKey,String endRowKey) throws IOException {

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));

        //扫描数据左闭右开，不包含endRowKey
        scan.withStopRow(Bytes.toBytes(endRowKey));

        Table table = connection.getTable(TableName.valueOf(tableName));

        List<HbasePojo> hbasePojoList = new ArrayList<>();

        try {
            ResultScanner scanner = table.getScanner(scan);

            for (Result result : scanner){

                String rowKey = Bytes.toString(result.getRow());
                hbasePojoList.addAll(this.getDataByRowKey(tableName,rowKey));
            }

            scanner.close();

            table.close();

            return hbasePojoList;

        } catch (IOException e) {

            log.error("scanData连接表失败:"+tableName+"startRowKey:"+startRowKey+"endRowKey"+endRowKey);
            throw new RuntimeException(e);
        }

    }

    /**
     * 删除数据，可选删除表、rowkey、列族、列、值
     *
     * @param tableName 要删除的数据的表名
     * @param rowKey
     * @param columnsFamily
     * @param columns
     * @param values
     * @throws IOException
     */
    @Deprecated
    public void deleteData(String tableName,@Nullable String rowKey,@Nullable String columnsFamily,@Nullable String[] columns,@Nullable String[] values) throws IOException {


        try {
            boolean isExists = this.tableExists(tableName);
            if (isExists == false){
                return;
            }
        } catch (IOException e) {

            log.error("deleteData 删除表失败,表："+tableName+"不存在");
            throw new RuntimeException(e);
        }

        if (rowKey == null){

            admin = connection.getAdmin();

            try {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                return;
            } catch (IOException e) {

                log.error("删除表失败："+tableName);
                throw new RuntimeException(e);
            }
        }
        //TODO 删除rowkey、列族、列、值


    }

    /**
     * 删除表
     *
     * @param tableName 要删除的表
     * @throws IOException 创建连接失败
     */
    public void deleteTable(String tableName) throws IOException {

        try {
            boolean isExists = this.tableExists(tableName);
            if (!isExists){
                return;
            }
        } catch (IOException e) {

            log.error("deleteData 删除表失败,表："+tableName+"不存在");
            throw new RuntimeException(e);
        }

        admin = connection.getAdmin();

        try {

            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

}
