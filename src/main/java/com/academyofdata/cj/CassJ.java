package com.academyofdata.cj;



import com.datastax.driver.core.*;

import java.util.List;
import java.util.ArrayList;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;

import static spark.Spark.*;

/**
 * Created by felix on 28/06/16.
 */
public class CassJ {
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE).setFetchSize(100)).build();
        Metadata cd = cluster.getMetadata();
        System.out.println("Connected to cluster:"+ cd.getClusterName());
        for ( Host host : cd.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();

    }

    public Session getSession() {
        return session;
    }

    public List<User> getData(){
        ResultSet results = session.execute("SELECT * FROM data.users limit 20");
        int count = 0;
        List<User> users = new ArrayList<>();
        //User[] users = new User[results.all().size()];
        for (Row row : results) {
            User u = new User();
            u.fromRow(row);
            users.add(u);
            System.out.println(String.format("%d:%s\t%s\t%s", ++count,row.getUUID("uid").toString(), row.getString("gender"),  row.getString("zip")));

        }
        return users;
    }

    public void getAsyncData(){
        ResultSetFuture future = session.executeAsync("SELECT * FROM data.users");

        while (!future.isDone()) {
            System.out.println("Waiting for request to complete");
        }

        //as an alternative to using future.isDone we could do
        //future.get(5, TimeUnit.SECONDS); - to wait for 5 seconds
        //and implement a catch for TimeoutException e in which we cancel the execution
        //future.cancel(true);


        try {
            ResultSet rs = future.get();
            int count = 0;
            for(Row row : rs) {
                System.out.println(String.format("%d:%s\t%s\t%s", ++count,row.getUUID("uid").toString(), row.getString("gender"),  row.getString("zip")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void close() {
        cluster.close();
        //cluster.shutdown();
    }

    public static void main(String[] args) {
        org.apache.log4j.BasicConfigurator.configure();
        String usersPath = "/users";
        CassJ client = new CassJ();
        client.connect("192.168.56.88");
        //client.getData();
        //client.getAsyncData();
        port(8080);
        UserModel.setSession(client.getSession());
        get(usersPath, (request, response) -> {

            response.type("application/json");
            List<User> users = client.getData();
            String resp = "[";
            for(User u: users){
                resp += u.toJson()+",";
            }
            return resp+"{}]";
        });

        post(usersPath, (request,response) -> {
            User u = User.fromJson(request.body());
             if(UserModel.save(u)) {
                response.status(201);
                return u.toJson();
            } else {
                response.status(500);
                return "{\"message\":\"fail\",\"error\":3}";
            }

        });

        get("/help", (request, response) -> "Help!");

        //client.close();
    }


}
