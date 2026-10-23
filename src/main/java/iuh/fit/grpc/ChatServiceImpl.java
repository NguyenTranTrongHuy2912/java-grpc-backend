package iuh.fit.grpc;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import iuh.fit.grpc.chat.ChatMessage;
import iuh.fit.grpc.chat.ChatResponse;
import iuh.fit.grpc.chat.ChatServiceGrpc;
import iuh.fit.grpc.chat.HistoryRequest;
import iuh.fit.grpc.chat.HistoryResponse;
import io.grpc.stub.StreamObserver;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private final MongoDatabase database = MongoConfig.getDatabase();
    private final MongoCollection<Document> messageCollection = database.getCollection("messages");

    public ChatServiceImpl() {
        messageCollection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("room_id"),
                Indexes.descending("timestamp")));
    }

    @Override
    public void sendMessage(ChatMessage request, StreamObserver<ChatResponse> responseObserver) {
        try {
            Document messageDoc = new Document()
                    .append("_id", request.getMessageId())
                    .append("room_id", request.getRoomId())
                    .append("sender_id", request.getSenderId())
                    .append("content", request.getContent())
                    .append("timestamp", request.getTimestamp());
            messageCollection.insertOne(messageDoc);

            responseObserver.onNext(ChatResponse.newBuilder()
                    .setMessageId(request.getMessageId())
                    .setIsSuccess(true)
                    .setStatusMessage("Đã lưu tin nhắn")
                    .setTimestamp(System.currentTimeMillis())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(ChatResponse.newBuilder()
                    .setMessageId(request.getMessageId())
                    .setIsSuccess(false)
                    .setStatusMessage("Lưu thất bại: " + e.getMessage())
                    .setTimestamp(System.currentTimeMillis())
                    .build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getMessageHistory(HistoryRequest request, StreamObserver<HistoryResponse> responseObserver) {
        int limit = request.getLimit() <= 0 ? 50 : Math.min(request.getLimit(), 100);

        Bson filter = Filters.eq("room_id", request.getRoomId());
        if (request.getBeforeTimestamp() > 0) {
            filter = Filters.and(filter, Filters.lt("timestamp", request.getBeforeTimestamp()));
        }

        List<Document> docs = messageCollection.find(filter)
                .sort(Sorts.descending("timestamp"))
                .limit(limit + 1)
                .into(new ArrayList<>());

        boolean hasMore = docs.size() > limit;
        if (hasMore) docs = docs.subList(0, limit);

        HistoryResponse.Builder builder = HistoryResponse.newBuilder().setHasMore(hasMore);
        for (Document d : docs) {
            builder.addMessages(ChatMessage.newBuilder()
                    .setMessageId(d.getString("_id"))
                    .setRoomId(d.getString("room_id"))
                    .setSenderId(d.getString("sender_id"))
                    .setContent(d.getString("content"))
                    .setTimestamp(d.getLong("timestamp")));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }
}