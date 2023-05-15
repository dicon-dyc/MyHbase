package com.dicon.myhbase.Pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HbasePojo implements Serializable {

    private String tableName;

    private String rowKey;

    private String columnsFamily;

    private String columns;

    private String values;



}
