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
import java.util.ArrayList;
import java.util.List;
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
class LoadBusiness {

    private final int batchSize = 1000;
    private int count = 0;
    private PreparedStatement psBusiness = null;
    private PreparedStatement psOpenHours = null;
    private PreparedStatement psSubCategories = null;
    private PreparedStatement psCategories;

    void load(String arg) {
        //PreparedStatement psFriend=null;

        BufferedReader read = null;
        Connection yelpConn = null;

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

            generatePreparedStatements(yelpConn);
            List<String> categories = getCategories();

            String record;
            System.out.println("populating business....");
            while ((record = read.readLine()) != null) {
                JSONObject jBusiness = (JSONObject) jsonParser.parse(record);
                String businessID = (String) jBusiness.get("business_id");
                boolean open = (boolean) jBusiness.get("open");
                String city = (String) jBusiness.get("city");
                String state = (String) jBusiness.get("state");
                long reviewCount = (long) jBusiness.get("review_count");
                String name = (String) jBusiness.get("name");
                double stars = (double) jBusiness.get("stars");
                psBusiness.setString(1, businessID);
                psBusiness.setString(2, name);
                if (open) {
                    psBusiness.setInt(3, 1);
                } else {
                    psBusiness.setInt(3, 0);
                }
                psBusiness.setString(4, city);
                psBusiness.setString(5, state);
                psBusiness.setLong(6, reviewCount);
                psBusiness.setDouble(7, stars);
                psBusiness.addBatch();
//                JSONObject hours = (JSONObject) jBusiness.get("hours");
//                if (hours != null) {
//                    loadOpenHours(businessID, hours);
//                }
                JSONArray jCategories = (JSONArray) jBusiness.get("categories");
                if (jCategories != null) {
                    loadCategoriesAndSubcategories(businessID, jCategories,categories);
                }
                if (++count % batchSize == 0) {
                    executeBatches();
                }
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LoadBusiness.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException | ParseException | SQLException ex) {
            Logger.getLogger(LoadBusiness.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (count % batchSize > 0) {
                    executeBatches();
                }
                System.out.println("businesses are populated.");
            } catch (SQLException ex) {
                Logger.getLogger(LoadBusiness.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

//    private void loadOpenHours(String businessID, JSONObject hours) throws SQLException {
//        String[] days = new String[]{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
//        for (String day : days) {
//            if (hours.containsKey(day)) {
//                JSONObject openHours = (JSONObject) hours.get(day);
//                String open = (String) openHours.get("open");
//                String close = (String) openHours.get("close");
//                psOpenHours.setString(1, businessID);
//                psOpenHours.setString(2, day);
//                psOpenHours.setString(3, open);
//                psOpenHours.setString(4, close);
//                psOpenHours.addBatch();
//            }
//        }
//    }

    private void loadCategoriesAndSubcategories(String businessID, JSONArray jCategories, List<String> categoriesList) throws SQLException {
        for(Object objCategory:jCategories){
            if(categoriesList.contains(String.valueOf(objCategory))){
                psCategories.setString(1, businessID);
                psCategories.setString(2, String.valueOf(objCategory));
                psCategories.addBatch();
            }
            else{
                psSubCategories.setString(1, businessID);
                psSubCategories.setString(2, String.valueOf(objCategory));
                psSubCategories.addBatch();
            }
        }
    }

    private void executeBatches() throws SQLException {
        psBusiness.executeBatch();
        psBusiness.clearBatch();
        //psOpenHours.executeBatch();
        //psOpenHours.clearBatch();
        psCategories.executeBatch();
        psCategories.clearBatch();
        psSubCategories.executeBatch();
        psSubCategories.clearBatch();
    }

    private void generatePreparedStatements(Connection yelpConn) throws SQLException {
        psBusiness = yelpConn.prepareStatement("Insert into business(business_id,business_name,OpenStatus,city,state,review_count,stars) values(?,?,?,?,?,?,?)");
        psOpenHours = yelpConn.prepareStatement("Insert into operation_days_of_business(business_id,day,open_time,close_time) values(?,?,?,?)");
        psSubCategories = yelpConn.prepareStatement("Insert into Subcategories(business_id,subcategory_name)values(?,?)");
        psCategories = yelpConn.prepareStatement("Insert into Categories(business_id,category_name)values(?,?)");
    }

    private List<String> getCategories() {
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
        return categories;
    }
}
