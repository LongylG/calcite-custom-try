package longyl.calcite.simpleCsv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Client {
    public static void main(String[] args) {
        try {

            String model = "{\"version\":\"1.0\",\"defaultSchema\":\"STUDENT\",\"schemas\":[{\"name\":\"STUDENT\",\"type\":\"custom\",\"factory\":\"longyl.calcite.simpleCsv.CustomSchemaFactory\",\"operand\":{}}]}";
            Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from student");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }



        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}