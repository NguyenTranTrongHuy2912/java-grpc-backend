package iuh.fit.grpc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.github.cdimascio.dotenv.Dotenv;

public class MongoConfig {
    // Tải thông tin cấu hình từ file .env
    private static final Dotenv dotenv = Dotenv.load();
    private static final String CONNECTION_STRING = dotenv.get("MONGO_URI");
    private static final String DATABASE_NAME = dotenv.get("MONGO_DB_NAME");

    private static MongoClient mongoClient = null;

    public static MongoDatabase getDatabase() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(CONNECTION_STRING);
            System.out.println(">>> Đã kết nối thành công tới MongoDB Database: " + DATABASE_NAME);
        }
        return mongoClient.getDatabase(DATABASE_NAME);
    }
}