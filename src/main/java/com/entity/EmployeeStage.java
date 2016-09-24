package com.entity;

/**
 * Created by vinod on 9/21/16.
 */
public class EmployeeStage {
    public EmployeeStage(String name, String dept, Double salary, boolean approved) {
        this.name = name;
        this.dept = dept;
        this.salary = salary;
        this.approved = approved;
    }

    public String name = "";

    public String dept = "";

    public Double salary;

    public boolean approved = false;

    public void setApproved(boolean approved) {
        this.approved = approved;
    }
}
