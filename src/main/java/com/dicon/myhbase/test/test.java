package com.dicon.myhbase.test;

import com.dicon.myhbase.Service.HBaseService;
import com.dicon.myhbase.config.HBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.io.IOException;

public class test {

    @Test
    public static void main(String[] args) {


        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("apitest");
            NamespaceDescriptor build = builder.build();
            admin.createNamespace(build);
            admin.close();

        } catch (IOException e) {
            throw new RuntimeException("连接失败");

        }
    }
}
