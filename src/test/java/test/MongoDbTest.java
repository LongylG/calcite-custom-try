package test;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MongoDbTest {

    /*
    init data
    db.user.insert({cp:{cid:1,name:'company1'},age:14,name:'gom1',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:1,name:'company1'},age:12,name:'jack2',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:2,name:'company2'},age:13,name:'Lily3',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:2,name:'company2'},age:14,name:'tony4',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:2,name:'company2'},age:9,name:'Harry5',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:2,name:'company2'},age:13,name:'Vincent6',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:1,name:'company1'},age:14,name:'bill7',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:2,name:'company2'},age:17,name:'tim8',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:3,name:'company3'},age:10,name:'bruce9',hobby:[{active:'music'},{active:'movie'}]});
    db.user.insert({cp:{cid:1,name:'company1'},age:20,name:'luxi10',hobby:[{active:'music'},{active:'movie'}]});
    */

    private static String HOST = "localhost";
    private static String DATABASE = "local";
    private static Connection connection;

    @Before
    public void init() {
        Properties info = new Properties();

        info.put("model",
                 "inline:" +
                     "{\n" +
                     "  \"version\": \"1.0\",\n" +
                     "  \"defaultSchema\": \"local\",\n" +
                     "  \"schemas\": [\n" +
                     "    {\n" +
                     "      type: 'custom',\n" +
                     "      name: 'local',\n" +
                     "      factory: 'org.apache.calcite.adapter.mongodb.MongoSchemaFactory',\n" +
                     "      operand: {\n" +
                     "        host: '" + HOST + "',\n" +
                     "        database: '" + DATABASE + "'\n" +
                     "      }\n" +
                     "    }\n" +
                     "  ]\n" +
                     "}");

        try {
            connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    ;

    @Test
    public void showDbShema() throws SQLException {
        ResultSet result = connection.getMetaData().getTables(null, null, null, null);
        while (result.next()) {
            System.out.println("Catalog : " + result.getString(1) + ",Database : " + result.getString(2) + ",Table : " + result.getString(3));
        }
        result.close();
    }

    @Test
    public void baseQuery() throws SQLException {
        ResultSet result = connection.prepareStatement("select _MAP['name'] from \"local\".\"user\" ").executeQuery();
        while (result.next()) {
            System.out.println("name:" + result.getString("name"));
        }
    }

    @Test
    public void orderAndPaginationQuery() throws SQLException {
        ResultSet resultSet = connection.prepareStatement("select _MAP['hobby'] hobby,cast( _MAP['age'] as int ) age,_MAP['name'] uname from \"user\" where _MAP['age'] = 20 order by age desc,name desc ").executeQuery();
        while (resultSet.next()) {
            System.out.println("hobby:" + resultSet.getString("hobby0") + ",age:" + resultSet.getString("age") + ",name:"
                                   + resultSet.getString("uname") + ",cid:" + resultSet.getString("cid"));
        }
        resultSet.close();
    }

    @Test
    public void nestedQuery() throws SQLException {
        ResultSet resultSet = connection.prepareStatement("select  cast (_MAP['cp.cid'] as int) cid from \"user\" where _MAP['cp.cid'] = 1 ").executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("cid"));
        }
    }

    @Test
    public void groupHavingQuery() throws SQLException {
        ResultSet resultSet = connection.prepareStatement("select _MAP['cp.cid'] as cid,count(*) as total from \"user\" group by _MAP['cp.cid'] ").executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("cid") + ",total:" + resultSet.getString("total"));
        }

    }


    @Test
    public void nativeQuery() {
        MongoClient mongo =
            new MongoClient(new ServerAddress("localhost"));
        MongoDatabase database = mongo.getDatabase("local");
        MongoCollection<Document> collection = database.getCollection("user");
        String sortString = "{\"age\":-1,\"name\":-1}";
        String eqString = "{},{'hobby':{$slice:-1}}";
        collection.find(BsonDocument.parse(eqString)).sort(BsonDocument.parse(sortString))
                  .forEach((Block<? super Document>) document ->
                      document.entrySet().stream()
                              .forEach(stringObjectEntry ->
                                           System.out.println(stringObjectEntry.getKey()
                                                                  + ":" + stringObjectEntry.getValue())));

    }

    @Test
    public void nativeAggQuery() {
        MongoClient mongo =
            new MongoClient(new ServerAddress("localhost"));
        MongoDatabase database = mongo.getDatabase("local");
        MongoCollection<Document> collection = database.getCollection("user");
        String aggString = "{$unwind:'$hobby'}";
        List<Bson> bsons = new ArrayList<>();
        bsons.add(BsonDocument.parse(aggString));
        collection.aggregate(bsons).forEach((Block<? super Document>) document ->
            document.entrySet().stream().forEach(stringObjectEntry -> {
                System.out.println(stringObjectEntry.getKey()
                                       + ":" + stringObjectEntry.getValue());
            }));
    }

}
