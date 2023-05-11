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

@Component
public class HBaseUtils {

    private Logger log =  LogManager.getLogger(com.dicon.myhbase.Utils.HBaseUtils.class);
    private Connection connection = null;

    private Admin admin = null;

    private Configuration configuration = null;


    private HBaseUtils(HBaseConfig hbaseConfig){

        try {
            Configuration configuration = hbaseConfig.getHBaseConfig();
            this.connection = ConnectionFactory.createConnection(configuration);
            this.admin = connection.getAdmin();
            log.info("---------------------admin" + admin);
        } catch (IOException e) {
            log.error("获取hbase连接失败:{}",e.getMessage(),e);
        }

    }


    /**
     *
     * @param nameSpace
     * @throws IOException
     */
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
