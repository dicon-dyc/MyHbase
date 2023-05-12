package com.dicon.myhbase.Utils;

import com.dicon.myhbase.config.HBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

        admin.createNamespace(build);

        admin.close();

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

            admin.close();


        }



    }
}
