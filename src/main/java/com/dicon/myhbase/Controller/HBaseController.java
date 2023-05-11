package com.dicon.myhbase.Controller;

import com.dicon.myhbase.Utils.HBaseUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;

@Controller
public class HBaseController {


    @Autowired
    HBaseUtils hbaseUtils;

    @GetMapping("/HBase/createNameSpace")
    public void createTable(@RequestParam("nameSpace") String nameSpace) throws IOException {

        hbaseUtils.createNamespace(nameSpace);


    }
}
