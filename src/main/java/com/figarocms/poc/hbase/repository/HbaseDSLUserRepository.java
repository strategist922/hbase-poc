package com.figarocms.poc.hbase.repository;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.figarocms.poc.hbase.dsl.types.DateTimeConverter;
import com.figarocms.poc.hbase.model.User;
import com.nearinfinity.hbase.dsl.Column;
import com.nearinfinity.hbase.dsl.ForEach;
import com.nearinfinity.hbase.dsl.HBase;
import com.nearinfinity.hbase.dsl.QueryOps;
import com.nearinfinity.hbase.dsl.Row;

/**
 * A {@link UserRepository} using the hbase-dsl client.
 * 
 * @author nhuray
 */
public class HbaseDSLUserRepository implements UserRepository {

    protected HBase<QueryOps<String>, String> hBase;

    public HbaseDSLUserRepository() {
        hBase = new HBase<QueryOps<String>, String>(String.class);
        hBase.defineTable(TABLE_NAME).family(FAM_INFO).family(FAM_FRIENDS);
        hBase.registerTypeConverter(new DateTimeConverter());
    }

    public User getUser(String id) {
        Row<String> row = hBase.fetch(TABLE_NAME).row(id);
        if (row != null) {
            User user = getUserInfo(row);
            final List<User> friends = new ArrayList<User>();
            row.family(FAM_FRIENDS).foreach(new ForEach<Column>() {
                public void process(Column col) {
                    String qualifier = col.qualifier();
                    String value = col.value(String.class);
                    Row<String> friendRow = hBase.fetch(value).row(qualifier);
                    friends.add(getUserInfo(friendRow));
                }
            });
            user.setFriends(friends.isEmpty() ? null : friends);
            return user;
        }
        return null;
    }

    private User getUserInfo(Row<String> row) {
        User user = new User();
        user.setId(row.getId());
        user.setName(row.value(FAM_INFO, COL_NAME, String.class));
        user.setAge(row.value(FAM_INFO, COL_AGE, Integer.class));
        user.setBirthdate(row.family(FAM_INFO).value(COL_BIRTHDATE, DateTime.class));
        return user;
    }

    public void saveUser(User user) {
        saveUserInfo(user);
        for (User friend : user.getFriends()) {
            saveUserInfo(friend);
            hBase.save(TABLE_NAME)//
                    .row(user.getId())//
                    .family(FAM_FRIENDS).col(friend.getId(), TABLE_NAME);
        }
        hBase.flush();
    }

    public void deleteUser(String id) {
        hBase.delete(TABLE_NAME).row(id).flush();
    }

    private void saveUserInfo(User user) {
        hBase.save(TABLE_NAME)//
                .row(user.getId())//
                .family(FAM_INFO)//
                .col(COL_NAME, user.getName())//
                .col(COL_AGE, user.getAge())//
                .col(COL_BIRTHDATE, user.getBirthdate().toString());
    }

}
