package com.figarocms.poc.hbase.repository;

import static com.figarocms.poc.hbase.repository.UserRepository.FAM_FRIENDS;
import static com.figarocms.poc.hbase.repository.UserRepository.FAM_INFO;
import static com.figarocms.poc.hbase.repository.UserRepository.TABLE_NAME;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.joda.time.format.DateTimeFormat.forPattern;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Before;
import org.junit.Test;

import com.figarocms.poc.hbase.BaseTest;
import com.figarocms.poc.hbase.model.User;

public abstract class BaseUserRepositoryTest extends BaseTest {

    private UserRepository reference;
    private UserRepository tested;

    public BaseUserRepositoryTest(UserRepository repository) {
        this.tested = repository;
        this.reference = new HbaseNativeUserRepository();
    }

    @Before
    public void setUp() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        if (!admin.tableExists(TABLE_NAME)) {
            HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
            desc.addFamily(new HColumnDescriptor(FAM_INFO));
            desc.addFamily(new HColumnDescriptor(FAM_FRIENDS));
            admin.createTable(desc);
        }
        hTable = new HTable(TABLE_NAME);
        assumeNotNull(tested);
    }

    @Test
    public void saveUser() throws Exception {

        // Given
        User userTest = getUserTest();

        // When
        tested.saveUser(userTest);
        User userRetrieved = reference.getUser(userTest.getId());

        // Then
        assertThat(userRetrieved, equalTo(userTest));
    }
    

    @Test
    public void getUser() throws Exception {
        // Given
        User userTest = getUserTest();
        reference.saveUser(userTest);

        // When
        User userRetrieved = tested.getUser(userTest.getId());

        // Then
        assertThat(userRetrieved, equalTo(userTest));
    }
    
    @Test
    public void deleteUser() throws Exception {
        // Given
        User userTest = getUserTest();
        reference.saveUser(userTest);
        
        // When
        tested.deleteUser(userTest.getId());
        User userRetrieved = reference.getUser(userTest.getId());
        
        // Then
        assertThat(userRetrieved, nullValue());
        for (User friend : userTest.getFriends()) {
            User friendRetrieved = reference.getUser(friend.getId());
            assertThat(friendRetrieved, equalTo(friend));
        }
    }

    // ~ Test data
    private User getUserTest() {
        User user = new User();
        user.setName("nicolas");
        user.setAge(31);
        user.setBirthdate(forPattern("dd/MM/yyyy").parseDateTime("26/01/1980"));
        user.setId("42");

        User david = new User();
        david.setName("david");
        david.setAge(2);
        david.setBirthdate(forPattern("dd/MM/yyyy").parseDateTime("29/03/1980"));
        david.setId("13");

        User laurent = new User();
        laurent.setName("laurent");
        laurent.setAge(31);
        laurent.setBirthdate(forPattern("dd/MM/yyyy").parseDateTime("28/05/1980"));
        laurent.setId("31");

        user.setFriends(asList(david, laurent));
        return user;
    }

}
