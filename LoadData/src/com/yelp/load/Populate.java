package com.yelp.load;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Keval
 */
public class Populate {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        Thread batchThread = new Thread(new BatchExecuter());
        batchThread.start();
        LoadUser loadUser = new LoadUser();
        loadUser.load(args[0]);
        //LoadCategories loadCategories = new LoadCategories();
        //loadCategories.load();
        LoadBusiness loadBusiness = new LoadBusiness();
        loadBusiness.load(args[1]);
        LoadReview loadReview = new LoadReview();
        loadReview.load(args[2]);
        LoadCheckin loadCheckin = new LoadCheckin();
        loadCheckin.load(args[3]);
        BatchExecuter.status=false;
    }

}
