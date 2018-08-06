package com.myspace.mytest;

/**
 * @author yujingze
 * @data 2018/8/6
 */
public class approve {

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCreditScore() {
        return creditScore;
    }

    public void setCreditScore(Integer creditScore) {
        this.creditScore = creditScore;
    }

    public Boolean getApproved() {
        return approved;
    }

    public void setApproved(Boolean approved) {
        this.approved = approved;
    }

    private String name;

    private Integer creditScore;

    private Boolean approved;

}
