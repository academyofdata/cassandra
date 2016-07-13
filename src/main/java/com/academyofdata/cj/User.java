package com.academyofdata.cj;

import com.google.gson.Gson;
import com.datastax.driver.core.Row;

import java.util.UUID;

/**
 * Created by felix on 12/07/16.
 */
public class User {
    public String zip;
    public int age;
    public String gender;
    public String occupation;
    public String uid;


    public User(){

    }

    public String toJson(){
        return new Gson().toJson(this);
    }

    public static User fromJson(String json){
        User u = null;
        try {
            u = new Gson().fromJson(json, User.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
         if(u.uid == null){
            u.uid = UUID.randomUUID().toString();
        }
        return u;
    }

    public boolean fromRow(Row row){
        this.uid = row.getUUID("uid").toString();
        this.age = row.getInt("age");
        this.gender = row.getString("gender");
        this.zip = row.getString("zip");
        this.occupation = row.getString("occupation");
        return true;
    }
}
