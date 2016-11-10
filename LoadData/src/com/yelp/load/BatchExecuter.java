/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yelp.load;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Keval
 */
public class BatchExecuter implements Runnable {

    //public static Queue<PreparedStatement> batches = (Queue<PreparedStatement>) new LinkedList<PreparedStatement>();
    public static boolean status = true;

    @Override
    public void run() {
//       synchronized(batches){
//        while (status) {
//
//            try {
//
//                PreparedStatement batch;
//                batch = batches.remove();
//                try {
//                    batch.executeBatch();
//                    batch.close();
//                } catch (SQLException ex) {
//                    Logger.getLogger(BatchExecuter.class.getName()).log(Level.SEVERE, null, ex);
//                }
//
//            } catch (NoSuchElementException ex) {
//                try {
//                    batches.wait();
//                } catch (InterruptedException ex1) {
//                    Logger.getLogger(BatchExecuter.class.getName()).log(Level.SEVERE, null, ex);
//                }
//            }
//        }
//    }
    }
}
