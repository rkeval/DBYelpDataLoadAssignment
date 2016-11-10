/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yelp.load;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Keval
 */
class LoadUser {

    public void load(String arg) {
        
        PreparedStatement psFriend=null;
        PreparedStatement psUser=null;
        BufferedReader read=null;
        Connection yelpConn=null;
        final int batchSize = 1000;
        int count = 0;
        try {
            JSONParser jsonParser = new JSONParser();
            System.out.println(System.getProperty("user.dir") + "\\" + arg);
            FileReader fileReader = new FileReader(System.getProperty("user.dir") + "\\" + arg);
            read = new BufferedReader(fileReader);

            //Connecting to the database
            yelpConn = DatabaseOpertions.getConnection();
            if (yelpConn == null) {
                System.out.println("Connection to the database is not established.");
                return;
            }

          
            psFriend = yelpConn.prepareStatement("Insert into friend_list(yelp_id,friend_id) values(?,?)");
           // psUser = yelpConn.prepareStatement("Insert into yelp_user(yelp_id,name,avg_star,sinceDate,review_count ) values(?,?,?,TO_DATE(?,'YYYY-MM-DD'),?)");
             psUser = yelpConn.prepareStatement("Insert into yelp_user(yelp_id,name,avg_star,sinceDate,review_count ) values(?,?,?,?,?)");
           
            String record;
            System.out.println("populating users....");
            while ((record = read.readLine()) != null) {
                JSONObject user = (JSONObject) jsonParser.parse(record);

                String userID = (String) user.get("user_id");
                //System.out.println("user_id: " + userID);

                String name = (String) user.get("name");
                //System.out.println("name: " + name);

                double avg_stars = (Double) user.get("average_stars");
                //System.out.println("avg_stars: " + avg_stars);

                String since = (String) user.get("yelping_since");
                java.util.Date dateSince = new SimpleDateFormat("yyyy-MM-dd").parse(since+ "-01");
                Date dtSince =new java.sql.Date(dateSince.getTime());
                //System.out.println("yelping_since: " + dtSince.toString());

                long review_count = (long) user.get("review_count");
                //System.out.println("review_count: " + review_count);

                psUser.setString(1, userID);
                psUser.setString(2, name);
                psUser.setDouble(3, avg_stars);
                psUser.setDate(4, dtSince);
                psUser.setLong(5, review_count);
              

                psUser.addBatch();
                if (++count % batchSize == 0) {
                    psUser.executeBatch();
                    //System.out.println(count);
                    psUser.clearBatch();
                }

                JSONArray friends = (JSONArray) user.get("friends");
                //System.out.println("Friends: ");
                for(Object ofriend:friends) {
                    String friendID= (String)ofriend;
                    psFriend.setString(1, userID);
                    psFriend.setString(2, friendID);
                    psFriend.addBatch();
                    //System.out.println(friendID + "");
                }
                //System.out.println("====================================================== ");
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | ParseException ex) {
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException | java.text.ParseException ex) {
            Logger.getLogger(LoadUser.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                read.close();
                if(count % batchSize>0)
                    psUser.executeBatch();
                 psUser.close();
                System.out.println(count+" Users are inserted. Populating Friends....");
                psFriend.executeBatch();
                psFriend.close();
                //BatchExecuter.batches.add(psFriend);
                System.out.println("Friends are inserted.");
                //yelpConn.close();
            } catch (SQLException | IOException ex) {
                Logger.getLogger(LoadUser.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
