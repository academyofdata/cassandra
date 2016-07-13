package com.academyofdata.cj;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;


/**
 * Created by felix on 12/07/16.
 */
public class UserModel {
    private static Session _session;

    private static final String _keyspace = "data";
    private static final String _table = "users";

    private static final String _select = "SELECT * FROM "+_keyspace+"."+_table;
    private static final String _where = " WHERE uid=?";

    public static void setSession(Session s){
        _session = s;
    }

    public static User findById(String uid) {
        Statement stmt = QueryBuilder.select().all().from(_keyspace,_table)
                .where(com.datastax.driver.core.querybuilder.QueryBuilder.eq("uid", UUID.fromString(uid)));
        ResultSet results = _session.execute(stmt);
        if(results.all().size()==0)
            return null;
        User x = new User();
        x.fromRow(results.one());
        return x;

    }

    public static boolean save(User u) {
        System.out.println(">>>>>>>>>>>>>>>>>>saving user "+u.toJson());
        Statement stmt = QueryBuilder.insertInto(_keyspace,_table)
                .value("uid",UUID.fromString(u.uid))
                .value("age",u.age)
                .value("ocupation",u.occupation)
                .value("zip",u.zip)
                .value("gender",u.gender)
                //.ifNotExists()
                .setConsistencyLevel(ConsistencyLevel.ONE);
        System.out.println(">>>>>>>>>>>>>>>>>>query built "+stmt.toString());
        try {
            ResultSet rs = _session.execute(stmt);

            System.out.println(">>>>>>>>>>>>>>>>>>> result " + rs.toString());
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
