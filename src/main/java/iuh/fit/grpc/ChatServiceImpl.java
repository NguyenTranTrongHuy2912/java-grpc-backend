package iuh.fit.grpc;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import iuh.fit.grpc.chat.ChatMessage;
import iuh.fit.grpc.chat.ChatResponse;
import iuh.fit.grpc.chat.ChatServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private final MongoDatabase database = MongoConfig.getDatabase();
    private final MongoCollection<Document> messageCollection = database.getCollection("messages");

    @Override
    public void sendMessage(ChatMessage request, StreamObserver<ChatResponse> responseObserver) {
        System.out.println("=== NHẬN REQUEST MỚI ===");
        System.out.println("Sender ID: " + request.getSenderId());
        System.out.println("Nội dung: " + request.getContent());

        // 1. Tạo Document lưu vào MongoDB
        Document messageDoc = new Document()
                .append("_id", request.getMessageId())
                .append("room_id", request.getRoomId())
                .append("sender_id", request.getSenderId())
                .append("content", request.getContent())
                .append("timestamp", request.getTimestamp());

        // 2. Chèn vào collection "messages"
        messageCollection.insertOne(messageDoc);
        System.out.println(">>> Đã lưu tin nhắn vào MongoDB Atlas thành công!");

        // 3. Trả về phản hồi cho gRPC Client (FastAPI)
        ChatResponse response = ChatResponse.newBuilder()
                .setMessageId(request.getMessageId())
                .setIsSuccess(true)
                .setStatusMessage("Java Backend & MongoDB Atlas đã lưu tin nhắn!")
                .setTimestamp(System.currentTimeMillis())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}