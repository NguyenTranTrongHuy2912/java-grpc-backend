package iuh.fit.grpc.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import java.util.concurrent.TimeUnit;

public class MongoConfig {
    private static final String CONNECTION_STRING = Env.require("MONGO_URI");
    private static final String DATABASE_NAME = Env.require("MONGO_DB_NAME");

    private static MongoClient mongoClient = null;

    public static synchronized MongoDatabase getDatabase() {
        if (mongoClient == null) {
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(CONNECTION_STRING))
                    // Gateway chờ gRPC tối đa 15 giây (GRPC_TIMEOUT). Mongo phải báo lỗi TRƯỚC mốc đó,
                    // nếu không gateway timeout "mù" trong khi Java vẫn chờ (mặc định 30 giây).
                    .applyToClusterSettings(b -> b.serverSelectionTimeout(8, TimeUnit.SECONDS))
                    .applyToSocketSettings(b -> b
                            .connectTimeout(8, TimeUnit.SECONDS)
                            .readTimeout(12, TimeUnit.SECONDS))
                    // Atlas/mạng hay cắt kết nối nhàn rỗi ("Connection reset"): bỏ kết nối cũ sớm,
                    // và kiểm tra trạng thái cluster thường xuyên hơn để thấy primary mới nhanh hơn.
                    .applyToConnectionPoolSettings(b -> b.maxConnectionIdleTime(60, TimeUnit.SECONDS))
                    .applyToServerSettings(b -> b.heartbeatFrequency(10, TimeUnit.SECONDS))
                    .retryWrites(true)
                    .retryReads(true)
                    .build();
            mongoClient = MongoClients.create(settings);
            System.out.println(">>> Đã kết nối thành công tới MongoDB Database: " + DATABASE_NAME);
        }
        return mongoClient.getDatabase(DATABASE_NAME);
    }
}