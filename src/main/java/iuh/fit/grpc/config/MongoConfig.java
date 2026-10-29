package iuh.fit.grpc.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.github.cdimascio.dotenv.Dotenv;

public class MongoConfig {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
    private static final String CONNECTION_STRING = require("MONGO_URI");
    private static final String DATABASE_NAME = require("MONGO_DB_NAME");

    private static MongoClient mongoClient = null;

    private static String require(String key) {
        String value = dotenv.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Thiếu biến cấu hình " + key + ". Hãy tạo file .env từ .env.example "
                            + "hoặc đặt biến này trong môi trường chạy.");
        }
        return value;
    }

    public static synchronized MongoDatabase getDatabase() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(CONNECTION_STRING);
            System.out.println(">>> Đã kết nối thành công tới MongoDB Database: " + DATABASE_NAME);
        }
        return mongoClient.getDatabase(DATABASE_NAME);
    }
}