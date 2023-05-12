package com.dicon.myhbase.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;


/**
 * @author dyc
 * @Date 2023/05/11
 */

@ConfigurationProperties(prefix = "hbase.zookeeper")
@org.springframework.context.annotation.Configuration
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HBaseConfig {

    private String quorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;


//    public Configuration getHBaseConfig(){
//
//        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum",quorum);
//        configuration.set("hbase.zookeeper.property.clientPort",clientPort);
//        return  configuration;
//    }

}
