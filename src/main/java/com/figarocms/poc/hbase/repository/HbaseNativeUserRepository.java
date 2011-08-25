package com.figarocms.poc.hbase.repository;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.joda.time.format.ISODateTimeFormat.dateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.figarocms.poc.hbase.model.User;

/**
 * A {@link UserRepository} using the native Hbase client api.
 * 
 * @author nhuray
 */
public class HbaseNativeUserRepository implements UserRepository {

    private static HTable hTable;

    public HbaseNativeUserRepository() {
        HBaseAdmin admin;
        try {
            admin = new HBaseAdmin(HBaseConfiguration.create());
            if (!admin.tableExists(TABLE_NAME)) {
                HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
                desc.addFamily(new HColumnDescriptor(FAM_INFO));
                desc.addFamily(new HColumnDescriptor(FAM_FRIENDS));
                admin.createTable(desc);
            }
            hTable = new HTable(TABLE_NAME);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void saveUser(User user) {
        List<Put> puts = new ArrayList<Put>();
        Put userPut = saveUserInfo(user);
        for (User friend : user.getFriends()) {
            Put friendPut = saveUserInfo(friend);
            puts.add(friendPut);
            userPut.add(Bytes.toBytes(FAM_FRIENDS), Bytes.toBytes(friend.getId()), Bytes.toBytes(TABLE_NAME));
        }
        puts.add(userPut);

        try {
            hTable.put(puts);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Put saveUserInfo(User user) {
        Put userPut = new Put(Bytes.toBytes(user.getId()));
        userPut.add(Bytes.toBytes(FAM_INFO), Bytes.toBytes(COL_NAME), Bytes.toBytes(user.getName()));
        userPut.add(Bytes.toBytes(FAM_INFO), Bytes.toBytes(COL_AGE), Bytes.toBytes(user.getAge()));
        userPut.add(Bytes.toBytes(FAM_INFO), Bytes.toBytes(COL_BIRTHDATE),
            Bytes.toBytes(user.getBirthdate().toString()));
        return userPut;
    }

    public User getUser(String id) {
        Get get = new Get(Bytes.toBytes(id));
        Result result;
        try {
            result = hTable.get(get);
            User user = getUserInfo(result);
            if (user != null) {
                NavigableMap<byte[], byte[]> famFriends = result.getFamilyMap(toBytes(FAM_FRIENDS));
                List<User> friends = new ArrayList<User>();
                for (Map.Entry<byte[], byte[]> entry : famFriends.entrySet()) {
                    Get friendGet = new Get(entry.getKey());
                    HTable friendTable = new HTable(entry.getValue());
                    Result friendResult = friendTable.get(friendGet);
                    friends.add(getUserInfo(friendResult));
                }
                user.setFriends(friends.isEmpty() ? null : friends);
                return user;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private User getUserInfo(Result result) {
        NavigableMap<byte[], byte[]> famInfo = result.getFamilyMap(toBytes(FAM_INFO));
        User user = null;
        if (famInfo != null && !famInfo.isEmpty()) {
            user = new User();
            user.setId(Bytes.toString(result.getRow()));
            user.setName(Bytes.toString(famInfo.get(toBytes(COL_NAME))));
            user.setAge(Bytes.toInt(famInfo.get(toBytes(COL_AGE))));
            user.setBirthdate(dateTime().parseDateTime(Bytes.toString(famInfo.get(toBytes(COL_BIRTHDATE)))));
        }
        return user;
    }

    public void deleteUser(String id) {
        Delete delete = new Delete(Bytes.toBytes(id));
        try {
            hTable.delete(delete);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}
