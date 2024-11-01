package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    
    private static final UserDaoJDBCImpl INSTANCE = new UserDaoJDBCImpl();
    
    private UserDaoJDBCImpl() {

    }
    
    public static UserDaoJDBCImpl getInstance() {
        return INSTANCE;
    }
    
    @Override
    public void createUsersTable() {
        String sql = """
                CREATE TABLE IF NOT EXISTS mydbtest.users
                (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(128) NOT NULL,
                    lastname VARCHAR(128) NOT NULL,
                    age TINYINT NOT NULL
                );
                """;
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void dropUsersTable() {
        String sql = """
                DROP TABLE IF EXISTS mydbtest.users;
                """;
        
        try (Connection connection = Util.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void saveUser(String name, String lastName, byte age) throws SQLException {
        String sql = """
                INSERT INTO mydbtest.users (name, lastname, age)
                VALUES (?, ? ,?);
                """;
        
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            connection.setAutoCommit(false);
            
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            
            connection.commit();
            System.out.println("User с именем — " + name + " добавлен в базу данных");
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
    
    @Override
    public void removeUserById(long id) throws SQLException {
        String sql = """
                DELETE FROM mydbtest.users WHERE id = ?;
                """;
        
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        
        try {
            connection = Util.getConnection();
            connection.setAutoCommit(false);
            
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            
            connection.commit();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
    
    @Override
    public List<User> getAllUsers() {
        String sql = """
                SELECT id,
                    name,
                    lastname,
                    age
                FROM mydbtest.users;
                """;
        try (Connection connection = Util.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            
            ResultSet resultSet = preparedStatement.executeQuery();
            List<User> users = new ArrayList<>();
            
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastname"));
                user.setAge(resultSet.getByte("age"));
                users.add(user);
            }
            return users;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void cleanUsersTable() throws SQLException {
        String sql = """
                DELETE FROM mydbtest.users;
                """;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = Util.getConnection();
            connection.setAutoCommit(false);
            
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            
            connection.commit();
        } catch (Exception e) {
            if (connection != null) {
                connection.rollback();
            }
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
            
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
}
