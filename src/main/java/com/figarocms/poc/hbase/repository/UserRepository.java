package com.figarocms.poc.hbase.repository;

import com.figarocms.poc.hbase.model.User;

public interface UserRepository {
    
    // ~ Table descriptor
    
    public static final String TABLE_NAME = "user";
    public static final String FAM_INFO = "info";
    public static final String FAM_FRIENDS = "friends";
    public static final String COL_BIRTHDATE = "birthdate";
    public static final String COL_AGE = "age";
    public static final String COL_NAME = "name";

   
    // ~ Methods
    
	User getUser(String id);
	
	void saveUser(User user);
	
	void deleteUser(String id);
	
}
