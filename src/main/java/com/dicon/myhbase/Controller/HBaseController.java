package com.dicon.myhbase.Controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dicon.myhbase.Utils.HBaseUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RestController
public class HBaseController {

    private final static Logger log = LogManager.getLogger(HBaseController.class);

    @Autowired
    HBaseUtils hbaseUtils;

    @GetMapping("/HBase/createNameSpace")
    @ResponseBody
    public void createNamespace(@RequestParam("nameSpace") String nameSpace) throws IOException {

        log.info("创建namespace: "+nameSpace);
        hbaseUtils.createNamespace(nameSpace);
    }


    @RequestMapping(value = "/HBase/createTable",method = RequestMethod.POST)
    @ResponseBody
    public void createTable(@RequestBody String jsonParam) throws IOException {

        JSONObject jsonObject = JSONObject.parseObject(jsonParam);

        String tableName = jsonObject.getString("tableName");

        List<String> families = JSONObject.parseArray(jsonObject.getJSONArray("families").toJSONString(),String.class);

        log.info("创建table: "+tableName+"列族: "+ Arrays.toString(families.toArray()));
        hbaseUtils.createTable(tableName,families);
    }
}
