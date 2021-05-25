package main.execution.catalog;

import java.util.HashMap;
import java.util.Map;

public class tableGenerator {
    String name;
    String typeId;
    Integer minValue;
    Integer maxValue;
    Map<String,String> nameToTypeId = new HashMap<>();
    Map<String,Integer> nameToDifference = new HashMap<>();

    public tableGenerator(String name, String typeId, Integer minValue, Integer maxValue) {
        this.name = name;
        this.typeId = typeId;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public Integer getMinValue() {
        return minValue;
    }

    public void setMinValue(Integer minValue) {
        this.minValue = minValue;
    }

    public Integer getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Integer maxValue) {
        this.maxValue = maxValue;
    }

    public void insertColumnMeta(String name, String typeId, Integer minValue, Integer maxValue){
        nameToTypeId.put(name,typeId);
        nameToDifference.put(name,maxValue-minValue);
    }
}
