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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Keval
 */
public class LoadCheckin {

    void load(String arg) {
        PreparedStatement psCheckin = null;
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
            psCheckin = yelpConn.prepareStatement("Insert into CHECKIN(business_id ,day ,hour ,checkin_count) values(?,?,?,?)");
            String record;
            System.out.println("populating checkin....");
            while ((record = read.readLine()) != null) {
                JSONObject review = (JSONObject) jsonParser.parse(record);
                String b_id = (String) review.get("business_id");
                JSONObject checkinInfo = (JSONObject) review.get("checkin_info");
                for (Object hourDayObject : checkinInfo.keySet()) {
                    String hourDay = String.valueOf(hourDayObject);
                    String[] separateValues = hourDay.split("-");
                    long checkin_count = (long) checkinInfo.get(hourDay);
                    psCheckin.setString(1, b_id);
                    psCheckin.setInt(2, Integer.parseInt(separateValues[1].trim()));
                    psCheckin.setInt(3, Integer.parseInt(separateValues[0].trim()));
                    psCheckin.setLong(4, checkin_count);
                    psCheckin.addBatch();
                    if (++count % batchSize == 0) {
                        psCheckin.executeBatch();
                        psCheckin.clearBatch();
                    }
                }

            }
        } catch (ParseException ex) {
            Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (count % batchSize > 0) {
                try {
                    psCheckin.executeBatch();
                    psCheckin.clearBatch();
                    psCheckin.close();
                    yelpConn.close();
                    read.close();
                    System.out.println("Checkin has been loaded...");
                } catch (SQLException ex) {
                    Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(LoadCheckin.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

}
