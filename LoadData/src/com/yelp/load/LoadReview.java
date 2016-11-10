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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Keval
 */
public class LoadReview {

    void load(String arg) {
        PreparedStatement psReview = null;
        BufferedReader read = null;
        Connection yelpConn = null;
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

            //reviewText = yelpConn.createClob();
            psReview = yelpConn.prepareStatement("Insert into REVIEWS(review_id,stars,votes,author,text,publish_date,business_id ) values(?,?,?,?,?,?,?)");
            String record;
            System.out.println("populating reviews....");
            while ((record = read.readLine()) != null) {
                JSONObject review = (JSONObject) jsonParser.parse(record);
                String userID = (String) review.get("user_id");
                String reviewID = (String) review.get("review_id");
                double stars = Double.parseDouble(review.get("stars").toString());
                String jReviewDate = (String) review.get("date");
                java.util.Date reviewDate = new SimpleDateFormat("yyyy-MM-dd").parse(jReviewDate);
                java.sql.Date reviewDt =new java.sql.Date(reviewDate.getTime());
                String jReviewText =  (String)review.get("text");
                //reviewText.setString(1, jReviewText);
                String businessID = (String) review.get("business_id");
                JSONObject jVotes = (JSONObject) review.get("votes");
                long votes = 0;
                if (jVotes != null) {
                    votes += (long) jVotes.get("useful");
                    votes += (long) jVotes.get("funny");
                    votes += (long) jVotes.get("cool");
                }
                psReview.setString(1, reviewID);
                psReview.setDouble(2, stars);
                psReview.setLong(3, votes);
                psReview.setString(4,userID);
                psReview.setString(5, jReviewText);
                psReview.setDate(6, reviewDt);
                psReview.setString(7, businessID);
                psReview.addBatch();
                //reviewText.truncate(jReviewText.length());
                if(++count % batchSize == 0){
                    psReview.executeBatch();
                    psReview.clearBatch();
                    //BatchExecuter.batches.add(psReview);
                    //psReview = yelpConn.prepareStatement("Insert into REVIEWS(review_id,stars,votes,author,text,publish_date,business_id ) values(?,?,?,?,?,?,?)");
            
                }
                
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LoadReview.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException | ParseException | IOException ex) {
            Logger.getLogger(LoadReview.class.getName()).log(Level.SEVERE, null, ex);
        } catch (java.text.ParseException ex) {
            Logger.getLogger(LoadReview.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                read.close();
                if(count % batchSize >0){
                psReview.executeBatch();
                psReview.clearBatch();
                }
                psReview.close();
                //reviewText.free();
                System.out.println("Reviews has been loaded successfully.");
            } catch (IOException | SQLException ex) {
                Logger.getLogger(LoadReview.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}