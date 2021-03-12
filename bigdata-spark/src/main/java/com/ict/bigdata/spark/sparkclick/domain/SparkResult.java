package com.ict.bigdata.spark.sparkclick.domain;

public class SparkResult {


    /**
     * 应用ID
     * */
    private String appid;
    /**
     * 结果状态 1 成功 -1 失败
     * */
    private int state;

    public SparkResult(String appid){
        this.appid=appid;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

}
