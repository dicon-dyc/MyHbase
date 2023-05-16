package com.dicon.myhbase.Controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dicon.myhbase.Pojo.HbasePojo;
import com.dicon.myhbase.Utils.HBaseUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author:dyc
 * @date:2023-05-13
 */
@RestController
public class HBaseController {

    private final static Logger log = LogManager.getLogger(HBaseController.class);

    @Autowired
    HBaseUtils hbaseUtils;

    /**
     * 创建namespace
     * ?nameSpace=dyc
     * @param nameSpace 需要创建的namespace
     * @throws IOException
     */
    @GetMapping("/HBase/createNameSpace")
    @ResponseBody
    public void createNamespace(@RequestParam("nameSpace") String nameSpace) throws IOException {

        log.info("创建namespace: "+nameSpace);
        hbaseUtils.createNamespace(nameSpace);
    }


    /**
     * 创建table
     * {
     *     "tableName":"dyc:testTable"
     *     ,"families":["Column Family 1","Column Family 2"]
     * }
     * @param createTableParam json格式输入的参数
     * @throws IOException json解析
     */
    @RequestMapping(value = "/HBase/createTable",method = RequestMethod.POST)
    @ResponseBody
    public void createTable(@RequestBody String createTableParam) throws IOException {

        JSONObject jsonObject = JSONObject.parseObject(createTableParam);

        String tableName = jsonObject.getString("tableName");

        List<String> families = JSONObject.parseArray(jsonObject.getJSONArray("families").toJSONString(),String.class);

        log.info("创建table: "+tableName+"列族: "+ Arrays.toString(families.toArray()));
        hbaseUtils.createTable(tableName,families);
    }


    /**
     * 插入或更新数据
     * {
     *     "tableName":"namespace:tablename"
     *     ,"rowKey":"rowkey"
     *     ,"columnsFamily":"columnsfamily"
     *     ,"columns":["column 1","column 2"]
     *     ,"values":["column 1  value","column 2 value"]
     * }
     * @param upSertDataParam json格式需要插入的数据
     */
    @RequestMapping(value = "/HBase/upsertData",method = RequestMethod.POST)
    @ResponseBody
    public void upsertData(@RequestBody String upSertDataParam){

        JSONObject jsonObject = JSONObject.parseObject(upSertDataParam);

        String tableName = jsonObject.getString("tableName");

        String rowKey = jsonObject.getString("rowKey");

        String columnFamily = jsonObject.getString("columnsFamily");

        String[] columns = jsonObject.getJSONArray("columns").toArray(new String[0]);

        String[] values = jsonObject.getJSONArray("values").toArray(new String[0]);

        hbaseUtils.upsertData(tableName,rowKey,columnFamily,columns,values);
    }

    /**
     * 通过rowkey获取数据
     *
     * {
     *     "tableName":"namespace:tablename"
     *     ,"rowKey":"rowkey"
     * }
     *
     * @param getDataByRowKeyParam json格式需要获取的数据的tablename & rowkey
     * @return HbasePojo 返回结果
     */
    @RequestMapping(value = "/HBase/getDataByRowkey",method = RequestMethod.POST)
    @ResponseBody
    public List<HbasePojo> getDataByRowkey(@RequestBody String getDataByRowKeyParam){

        JSONObject jsonObject = JSONObject.parseObject(getDataByRowKeyParam);

        String tableName = jsonObject.getString("tableName");

        String rowKey = jsonObject.getString("rowKey");

        return hbaseUtils.getDataByRowKey(tableName,rowKey);

    }

    /**
     * rowkey范围查询数据
     * {
     *     "tableName":"dyc:testTable"
     *     ,"startRowKey":"Column Family 1:col1"
     *     ,"endRowKey":"dyc02"
     * }
     *
     * @param scanDataParam json格式需要获取的tablename 和 startrowkey & endrowkey
     * @return 返回查询结果
     * @throws IOException 创建连接失败
     */
    @RequestMapping(value = "/HBase/scanData",method = RequestMethod.POST)
    @ResponseBody
    public List<HbasePojo> scanData(@RequestBody String scanDataParam) throws IOException {

        JSONObject jsonObject = JSONObject.parseObject(scanDataParam);

        String tableName = jsonObject.getString("tableName");

        String startRowKey = jsonObject.getString("startRowKey");

        String endRowKey = jsonObject.getString("endRowKey");

        return hbaseUtils.scanData(tableName,startRowKey,endRowKey);
    }

    /**
     * 删除表
     *
     *
     * @param deleteTableParam json格式需要获取的tableName
     * @throws IOException
     */
    @RequestMapping("/HBase/deleteTable")
    @ResponseBody
    public void deleteTable(@RequestBody String deleteTableParam) throws IOException {

        JSONObject jsonObject = JSONObject.parseObject(deleteTableParam);

        String tableName = jsonObject.getString("tableName");

        hbaseUtils.deleteTable(tableName);
    }

}
