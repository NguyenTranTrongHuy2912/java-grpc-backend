package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.ChatEvent;
import iuh.fit.grpc.chat.EventType;
import iuh.fit.grpc.chat.FriendRequestItem;
import iuh.fit.grpc.chat.FriendRequestStatus;
import iuh.fit.grpc.chat.ListFriendRequestsReq;
import iuh.fit.grpc.chat.ListFriendRequestsResp;
import iuh.fit.grpc.chat.ListFriendsReq;
import iuh.fit.grpc.chat.ListFriendsResp;
import iuh.fit.grpc.chat.RespondFriendRequestReq;
import iuh.fit.grpc.chat.RespondFriendRequestResp;
import iuh.fit.grpc.chat.SendFriendRequestReq;
import iuh.fit.grpc.chat.SendFriendRequestResp;
import iuh.fit.grpc.chat.UserSummary;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Hệ thống kết bạn: gửi/trả lời lời mời, danh sách bạn bè, danh sách lời mời đang chờ. */
public class FriendService {
    private static final Logger log = LoggerFactory.getLogger(FriendService.class);

    private final MongoCollection<Document> userCollection;
    private final MongoCollection<Document> friendRequestCollection;
    private final MongoCollection<Document> friendshipCollection;
    private final RealtimeHub hub;

    public FriendService(MongoDatabase db, RealtimeHub hub) {
        this.userCollection = db.getCollection("users");
        this.friendRequestCollection = db.getCollection("friend_requests");
        this.friendshipCollection = db.getCollection("friendships");
        this.hub = hub;
    }

    /** Gọi từ ChatServiceImpl.ensureIndexes (chạy nền một lần lúc khởi động). */
    public void ensureIndexes() {
            friendRequestCollection.createIndex(Indexes.compoundIndex(
                    Indexes.ascending("receiver_id"),
                    Indexes.ascending("status")));
            friendshipCollection.createIndex(Indexes.compoundIndex(
                            Indexes.ascending("user_id"),
                            Indexes.ascending("friend_id")),
                    new IndexOptions().unique(true));
    }

    public void sendFriendRequest(SendFriendRequestReq request, StreamObserver<SendFriendRequestResp> out) {
        String senderId = request.getSenderId();
        String receiverId = request.getReceiverId();

        if (senderId.isBlank() || receiverId.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu sender_id hoặc receiver_id").asRuntimeException());
            return;
        }
        if (senderId.equals(receiverId)) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Không thể kết bạn với chính mình").asRuntimeException());
            return;
        }

        try {
            // Kiểm tra đã là bạn bè chưa
            Document existingFriendship = friendshipCollection.find(
                    Filters.and(Filters.eq("user_id", senderId), Filters.eq("friend_id", receiverId))
            ).first();
            if (existingFriendship != null) {
                out.onError(Status.ALREADY_EXISTS.withDescription("Hai người đã là bạn bè").asRuntimeException());
                return;
            }

            // Kiểm tra đã có lời mời đang chờ chưa
            Document pending = friendRequestCollection.find(
                    Filters.and(
                            Filters.eq("sender_id", senderId),
                            Filters.eq("receiver_id", receiverId),
                            Filters.eq("status", "PENDING")
                    )
            ).first();
            if (pending != null) {
                out.onError(Status.ALREADY_EXISTS.withDescription("Đã gửi lời mời kết bạn trước đó").asRuntimeException());
                return;
            }

            // Lấy thông tin người gửi
            Document senderDoc = userCollection.find(Filters.eq("_id", senderId)).first();
            String senderName = senderDoc != null && senderDoc.getString("display_name") != null
                    ? senderDoc.getString("display_name") : "Người dùng";
            String senderEmail = senderDoc != null && senderDoc.getString("account") != null
                    ? senderDoc.getString("account") : "";

            String requestId = UUID.randomUUID().toString();
            long now = System.currentTimeMillis();

            Document reqDoc = new Document()
                    .append("_id", requestId)
                    .append("sender_id", senderId)
                    .append("sender_name", senderName)
                    .append("sender_email", senderEmail)
                    .append("receiver_id", receiverId)
                    .append("created_at", now)
                    .append("status", "PENDING");
            friendRequestCollection.insertOne(reqDoc);

            FriendRequestItem item = FriendRequestItem.newBuilder()
                    .setRequestId(requestId)
                    .setSenderId(senderId)
                    .setSenderName(senderName)
                    .setSenderEmail(senderEmail)
                    .setReceiverId(receiverId)
                    .setCreatedAt(now)
                    .setStatus(FriendRequestStatus.FRIEND_REQUEST_PENDING)
                    .build();

            // Realtime push cho receiver
            hub.pushToUser(receiverId, ChatEvent.newBuilder()
                    .setEventType(EventType.EVENT_FRIEND_REQUEST)
                    .setFriendRequest(item)
                    .setTimestamp(now)
                    .build());

            out.onNext(SendFriendRequestResp.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Đã gửi lời mời kết bạn")
                    .setRequest(item)
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Gửi lời mời kết bạn thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không gửi được lời mời kết bạn").asRuntimeException());
        }
    }

    public void respondFriendRequest(RespondFriendRequestReq request, StreamObserver<RespondFriendRequestResp> out) {
        String userId = request.getUserId();
        String reqId = request.getRequestId();
        boolean accept = request.getAccept();

        if (userId.isBlank() || reqId.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu thông tin").asRuntimeException());
            return;
        }

        try {
            Document reqDoc = friendRequestCollection.find(
                    Filters.and(
                            Filters.eq("_id", reqId),
                            Filters.eq("receiver_id", userId),
                            Filters.eq("status", "PENDING")
                    )
            ).first();

            if (reqDoc == null) {
                out.onError(Status.NOT_FOUND.withDescription("Không tìm thấy lời mời kết bạn hoặc đã được xử lý").asRuntimeException());
                return;
            }

            String senderId = reqDoc.getString("sender_id");
            long now = System.currentTimeMillis();

            if (accept) {
                friendRequestCollection.updateOne(
                        Filters.eq("_id", reqId),
                        Updates.set("status", "ACCEPTED")
                );

                // Thêm quan hệ bạn bè 2 chiều
                friendshipCollection.insertOne(new Document()
                        .append("user_id", userId)
                        .append("friend_id", senderId)
                        .append("created_at", now));
                friendshipCollection.insertOne(new Document()
                        .append("user_id", senderId)
                        .append("friend_id", userId)
                        .append("created_at", now));

                // Realtime notify sender that their request was accepted
                hub.pushToUser(senderId, ChatEvent.newBuilder()
                        .setEventType(EventType.EVENT_FRIEND_ACCEPTED)
                        .setUserId(userId)
                        .setTimestamp(now)
                        .build());
            } else {
                friendRequestCollection.updateOne(
                        Filters.eq("_id", reqId),
                        Updates.set("status", "REJECTED")
                );
            }

            out.onNext(RespondFriendRequestResp.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage(accept ? "Đã chấp nhận kết bạn" : "Đã từ chối lời mời")
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Xử lý lời mời kết bạn thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không xử lý được lời mời").asRuntimeException());
        }
    }

    public void listFriends(ListFriendsReq request, StreamObserver<ListFriendsResp> out) {
        String userId = request.getUserId();
        if (userId.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu user_id").asRuntimeException());
            return;
        }

        try {
            List<Document> friendships = friendshipCollection.find(Filters.eq("user_id", userId)).into(new ArrayList<>());
            List<UserSummary> list = new ArrayList<>();
            for (Document f : friendships) {
                String friendId = f.getString("friend_id");
                if (friendId != null) {
                    Document u = userCollection.find(Filters.eq("_id", friendId)).first();
                    if (u != null) {
                        list.add(UserSummary.newBuilder()
                                .setUserId(friendId)
                                .setDisplayName(u.getString("display_name") != null ? u.getString("display_name") : "")
                                .setEmail(u.getString("account") != null ? u.getString("account") : "")
                                .setAvatarUrl(u.getString("avatar_url") != null ? u.getString("avatar_url") : "")
                                .build());
                    }
                }
            }

            out.onNext(ListFriendsResp.newBuilder().addAllFriends(list).build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Liệt kê bạn bè thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không lấy được danh sách bạn bè").asRuntimeException());
        }
    }

    public void listFriendRequests(ListFriendRequestsReq request, StreamObserver<ListFriendRequestsResp> out) {
        String userId = request.getUserId();
        if (userId.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu user_id").asRuntimeException());
            return;
        }

        try {
            List<Document> requests = friendRequestCollection.find(
                    Filters.and(
                            Filters.eq("receiver_id", userId),
                            Filters.eq("status", "PENDING")
                    )
            ).sort(Sorts.descending("created_at")).into(new ArrayList<>());

            List<FriendRequestItem> items = new ArrayList<>();
            for (Document d : requests) {
                items.add(FriendRequestItem.newBuilder()
                        .setRequestId(d.getString("_id"))
                        .setSenderId(d.getString("sender_id"))
                        .setSenderName(d.getString("sender_name") != null ? d.getString("sender_name") : "")
                        .setSenderEmail(d.getString("sender_email") != null ? d.getString("sender_email") : "")
                        .setReceiverId(userId)
                        .setCreatedAt(d.getLong("created_at") != null ? d.getLong("created_at") : 0L)
                        .setStatus(FriendRequestStatus.FRIEND_REQUEST_PENDING)
                        .build());
            }

            out.onNext(ListFriendRequestsResp.newBuilder().addAllRequests(items).build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Liệt kê lời mời kết bạn thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không lấy được danh sách lời mời").asRuntimeException());
        }
    }
}
