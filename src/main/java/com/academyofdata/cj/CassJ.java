package com.academyofdata.cj;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
/**
 * Created by felix on 28/06/16.
 */
public class CassJ {
    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node).build();
        Metadata cd = cluster.getMetadata();
        System.out.println("Connected to cluster:"+ cd.getClusterName());
        for ( Host host : cd.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void getData(){
        ResultSet results = session.execute("SELECT * FROM data.users");
        int count = 0;
        for (Row row : results) {
            System.out.println(String.format("%d:%s\t%s\t%s", ++count,row.getUUID("uid").toString(), row.getString("gender"),  row.getString("zip")));
        }
    }

    public void getAsyncData(){
        ResultSetFuture future = session.executeAsync("SELECT * FROM data.users");

        while (!future.isDone()) {
            System.out.println("Waiting for request to complete");
        }

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
        CassJ client = new CassJ();
        client.connect("192.168.56.88");
        //client.getData();
        client.getAsyncData();
        client.close();
    }


}
