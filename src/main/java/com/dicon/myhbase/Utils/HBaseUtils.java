package com.dicon.myhbase.Utils;

import com.dicon.myhbase.config.HBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;


public class HBaseUtils {

    private Logger log =  LogManager.getLogger(com.dicon.myhbase.Utils.HBaseUtils.class);
    private Connection connection = null;

    private Admin admin = null;

    private Configuration configuration = null;

    private static com.dicon.myhbase.Utils.HBaseUtils HBaseUtils = new HBaseUtils();


    private HBaseConfig HBaseConfig = new HBaseConfig();

    private HBaseUtils(){

        try {
            Configuration configuration = HBaseConfig.getHBaseConfig();
            this.connection = ConnectionFactory.createConnection(configuration);
            this.admin = connection.getAdmin();
            log.info("---------------------admin" + admin);
        } catch (IOException e) {
            log.error("获取hbase连接失败!");
        }

    }

    public static com.dicon.myhbase.Utils.HBaseUtils getHBaseUtils(){

        return HBaseUtils;

    }


    public void createNamespace(String nameSpace) throws IOException {

        admin = connection.getAdmin();

        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);

        NamespaceDescriptor build = builder.build();

        admin.createNamespace(build);

        admin.close();



    }


    public void createTable(String tableName, String... columnFamilies) {

    }
}
