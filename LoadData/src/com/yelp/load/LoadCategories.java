/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yelp.load;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.parser.JSONParser;

/**
 *
 * @author Keval
 */
class LoadCategories {

    void load() {
       
        PreparedStatement psCategories=null;
        Connection yelpConn=null;
        final int batchSize = 1000;
        int count = 0;
        try {
           List<String> categories = new ArrayList<String>();
           categories.add("Active Life");
           categories.add("Arts & Entertainment");
           categories.add("Automotive");
           categories.add("Car Rental");
           categories.add("Cafes");
           categories.add("Beauty & Spas");
           categories.add("Convenience Stores");
           categories.add("Dentists");
           categories.add("Doctors");
           categories.add("Drugstores");
           categories.add("Department Stores");
           categories.add("Education");
           categories.add("Event Planning & Services");
           categories.add("Flowers & Gifts");
           categories.add("Food");
           categories.add("Health & Medical");
           categories.add("Home Services");
           categories.add("Home & Garden");
           categories.add("Hospitals");
           categories.add("Hotels & Travel");
           categories.add("Hardware Stores");
           categories.add("Grocery");
           categories.add("Medical Centers");
           categories.add("Nurseries & Gardening");
           categories.add("Nightlife");
           categories.add("Restaurants");
           categories.add("Shopping");
           categories.add("Transportation");
           
           yelpConn= DatabaseOpertions.getConnection();
           psCategories = yelpConn.prepareStatement("insert into business_category(business_category_id,business_category_name) values(?,?)");
           int id =0;
           for(String category:categories){
               psCategories.setString(1, String.valueOf(++id));
               psCategories.setString(2, category);
               psCategories.addBatch();
           }
           psCategories.executeBatch();
        }
        catch(SQLException ex){
            Logger.getLogger(Populate.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                yelpConn.close();
            } catch (SQLException ex) {
                Logger.getLogger(LoadCategories.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
}
