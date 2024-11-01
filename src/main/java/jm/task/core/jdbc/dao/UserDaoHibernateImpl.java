package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.*;
import org.hibernate.query.NativeQuery;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    
    private static final UserDaoHibernateImpl INSTANCE = new UserDaoHibernateImpl();
    
    private UserDaoHibernateImpl() {

    }
    
    public static UserDaoHibernateImpl getInstance() {
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
        
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            session.createNativeQuery(sql).executeUpdate();
            
            transaction.commit();
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropUsersTable() {
        String sql = """
                DROP TABLE IF EXISTS mydbtest.users;
                """;
        
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            session.createNativeQuery(sql).executeUpdate();
            
            transaction.commit();
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            User user = new User(name, lastName, age);
            session.save(user);
            
            transaction.commit();
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            User user = session.get(User.class, id);
            if (user != null) {
                session.delete(user);
            }
            
            transaction.commit();
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw new RuntimeException(e);
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
        
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            List<User> users = session.createNativeQuery(sql, User.class).getResultList();
            
            transaction.commit();
            return users;
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanUsersTable() {
        String sql = """
                DELETE FROM mydbtest.users;
                """;
        
        Transaction transaction = null;
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            
            session.createNativeQuery(sql).executeUpdate();

            transaction.commit();
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw new RuntimeException(e);
        }
    }
}
