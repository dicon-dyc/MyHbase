package com.dicon.myhbase.config;

import com.dicon.myhbase.Service.HBaseService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HBaseConfig {

    @Bean
    public HBaseService getHBaseService(){

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.56.102,192.168.56.103,192.168.56.104");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        return new HBaseService(configuration);
    }

}
