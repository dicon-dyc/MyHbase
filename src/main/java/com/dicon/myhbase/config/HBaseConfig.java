package com.dicon.myhbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


/**
 * @author dyc
 * @Date 2023/05/11
 */
@org.springframework.context.annotation.Configuration
public class HBaseConfig {

    @Value("hbase.zookeeper.quorum")
    private String hbase_zookeeper_quorum;

    @Value("hbase.zookeeper.property.clientPort")
    private String hbase_zookeeper_property_clientPort;


    public Configuration getHBaseConfig(){

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",hbase_zookeeper_quorum);
        configuration.set("hbase.zookeeper.property.clientPort",hbase_zookeeper_property_clientPort);
        return  configuration;
    }

}
