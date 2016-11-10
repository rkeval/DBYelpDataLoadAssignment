/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yelp.load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Keval
 */
class DatabaseOpertions {

    private static Connection dbConnection=null;

    static Connection getConnection(){
        if(dbConnection!=null)
            return dbConnection;
        try{
            Class.forName("oracle.jdbc.driver.OracleDriver");
            dbConnection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "SYSTEM", "manager");
        }
        catch(ClassNotFoundException | SQLException ex){
             Logger.getLogger(LoadUser.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dbConnection;  
    }
    
}
