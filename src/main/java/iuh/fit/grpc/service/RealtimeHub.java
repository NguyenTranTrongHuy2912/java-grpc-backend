package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.ChatEvent;
import iuh.fit.grpc.chat.EventType;
import iuh.fit.grpc.chat.Room;
import iuh.fit.grpc.chat.StreamEventsRequest;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Quản lý các stream realtime (SubscribeEvents) và phát event tới user hoặc cả phòng.
 * Đây là nơi duy nhất giữ danh sách subscriber. Muốn chạy nhiều instance Java sau này
 * (đẩy qua Redis pub/sub) thì chỉ cần thay class này.
 */
public class RealtimeHub {
    private static final Logger log = LoggerFactory.getLogger(RealtimeHub.class);

    private final MongoCollection<Document> rooms;

    // Active subscribers theo user_id
    private final Map<String, Set<StreamObserver<ChatEvent>>> userSubscribers = new ConcurrentHashMap<>();

    public RealtimeHub(MongoDatabase db) {
        this.rooms = db.getCollection("rooms");
    }

    public void subscribe(StreamEventsRequest request, StreamObserver<ChatEvent> responseObserver) {
        String userId = request.getUserId();
        if (userId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Thiếu user_id để subscribe").asRuntimeException());
            return;
        }

        addSubscriber(userId, responseObserver);
        log.info("User {} đã kết nối realtime event stream", userId);

        if (responseObserver instanceof ServerCallStreamObserver<ChatEvent> serverObserver) {
            serverObserver.setOnCancelHandler(() -> {
                removeSubscriber(userId, responseObserver);
                log.info("User {} đã hủy realtime event stream", userId);
            });
        }
    }

    /**
     * Thêm stream của user. Dùng compute để thao tác atomic với removeSubscriber: trước đây
     * "get set -> kiểm tra rỗng -> remove(userId)" của luồng hủy có thể xóa mất set vừa được
     * luồng kết nối lại thêm stream mới vào, khiến user nối lại rồi nhưng không nhận được event nào.
     */
    private void addSubscriber(String userId, StreamObserver<ChatEvent> obs) {
        userSubscribers.compute(userId, (k, set) -> {
            if (set == null) set = ConcurrentHashMap.newKeySet();
            set.add(obs);
            return set;
        });
    }

    private void removeSubscriber(String userId, StreamObserver<ChatEvent> obs) {
        userSubscribers.computeIfPresent(userId, (k, set) -> {
            set.remove(obs);
            return set.isEmpty() ? null : set;
        });
    }

    /** Gửi event tới mọi stream đang mở của một user. */
    public void pushToUser(String userId, ChatEvent event) {
        Set<StreamObserver<ChatEvent>> subscribers = userSubscribers.get(userId);
        if (subscribers == null || subscribers.isEmpty()) return;

        for (StreamObserver<ChatEvent> obs : subscribers) {
            if (obs instanceof ServerCallStreamObserver
                    && ((ServerCallStreamObserver<ChatEvent>) obs).isCancelled()) {
                removeSubscriber(userId, obs);
                continue;
            }
            try {
                // StreamObserver của gRPC không an toàn đa luồng: các tin gửi cùng lúc phải xếp hàng theo từng stream
                synchronized (obs) {
                    obs.onNext(event);
                }
            } catch (Exception ex) {
                log.warn("Gửi realtime event cho user {} lỗi, đóng stream để app kết nối lại", userId, ex);
                removeSubscriber(userId, obs);
                try {
                    // Báo cho gateway biết stream đã hỏng, nếu không nó vẫn giữ WebSocket mở nhưng không nhận được gì
                    obs.onError(Status.UNAVAILABLE.withDescription("Stream realtime lỗi").asRuntimeException());
                } catch (Exception ignored) {
                    // stream đã đóng sẵn
                }
            }
        }
    }

    public void broadcastToRoom(String roomId, ChatEvent event) {
        try {
            Document room = rooms.find(Filters.eq("_id", roomId)).first();
            if (room == null) return;
            List<String> memberIds = room.getList("member_ids", String.class);
            if (memberIds == null || memberIds.isEmpty()) return;

            for (String memberId : memberIds) {
                pushToUser(memberId, event);
            }
        } catch (Exception e) {
            log.error("Lỗi khi phát tán realtime event cho phòng {}", roomId, e);
        }
    }

    public static ChatEvent roomUpdatedEvent(String roomId) {
        return ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_ROOM_UPDATED)
                .setRoom(Room.newBuilder().setRoomId(roomId))
                .setTimestamp(System.currentTimeMillis())
                .build();
    }
}