package com.example.bigdataSpark.sparkClick.domain;

public class sparkResult {


    /**
     * 应用ID
     * */
    private String appid;
    /**
     * 结果状态 1 成功 -1 失败
     * */
    private int state;

    public sparkResult(String appid){
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
