package com.dicon.myhbase.Service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;

public class HBaseService {


    private Logger log = (Logger) LogManager.getLogger(HBaseService.class);

    private Admin admin = null;

    private Connection connection = null;

    public HBaseService(Configuration configuration){

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            log.info("---------------------admin" + admin);
        } catch (IOException e) {
            log.error("获取hbase连接失败!");
        }

    }

//    /**
//     *
//     * @param tableName:创建的表名
//     * @param columnFamily:列族列表
//     * @return
//     */
//    public boolean creatTable(String tableName, List<String> columnFamily){
//
//        try {
//
//            //列族column family
//            List<ColumnFamilyDescriptor>
//
//        }
//
//    }
}
