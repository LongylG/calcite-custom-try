package test;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import javax.sql.DataSource;

public class JDBCTest {
    public static void main(String[] args) {
        try {

            Properties info = new Properties();
            DataSource dataSource = JdbcSchema.dataSource("jdbc:postgresql://172.19.79.8:26257/khipu?useUnicode=true&sslmode=require&characterEncoding=UTF-8",
                                             "org.postgresql.Driver", "fuyun", "fuyun2019");

            Connection connection = dataSource.getConnection();
            //get database shema
            ResultSet result = connection.getMetaData().getTables(null, null, null, null);
            while (result.next()) {
                System.out.println("Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result.getString(3));
            }
            result.close();

            ResultSet result2 = connection.prepareStatement("select id from collections").executeQuery();
            while (result2.next()){
                System.out.println(result2.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
