package iuh.fit.grpc.service;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.ChatMessage;
import iuh.fit.grpc.chat.ChatResponse;
import iuh.fit.grpc.chat.ChatServiceGrpc;
import iuh.fit.grpc.chat.CreateRoomRequest;
import iuh.fit.grpc.chat.HistoryRequest;
import io.grpc.stub.ServerCallStreamObserver;
import iuh.fit.grpc.chat.ChatEvent;
import iuh.fit.grpc.chat.EventType;
import iuh.fit.grpc.chat.StreamEventsRequest;
import java.util.concurrent.ConcurrentHashMap;
import iuh.fit.grpc.chat.FriendRequestItem;
import iuh.fit.grpc.chat.FriendRequestStatus;
import iuh.fit.grpc.chat.SendFriendRequestReq;
import iuh.fit.grpc.chat.SendFriendRequestResp;
import iuh.fit.grpc.chat.RespondFriendRequestReq;
import iuh.fit.grpc.chat.RespondFriendRequestResp;
import iuh.fit.grpc.chat.ListFriendsReq;
import iuh.fit.grpc.chat.ListFriendsResp;
import iuh.fit.grpc.chat.ListFriendRequestsReq;
import iuh.fit.grpc.chat.ListFriendRequestsResp;
import iuh.fit.grpc.chat.HistoryResponse;
import iuh.fit.grpc.chat.ListRoomsRequest;
import iuh.fit.grpc.chat.ListRoomsResponse;
import iuh.fit.grpc.chat.MarkRoomReadRequest;
import iuh.fit.grpc.chat.MarkRoomReadResponse;
import iuh.fit.grpc.chat.MessageType;
import iuh.fit.grpc.chat.Room;
import iuh.fit.grpc.chat.RoomMember;
import iuh.fit.grpc.chat.RoomType;
import iuh.fit.grpc.chat.SearchUsersRequest;
import iuh.fit.grpc.chat.SearchUsersResponse;
import iuh.fit.grpc.chat.UserSummary;
import iuh.fit.grpc.config.MongoConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ChatServiceImpl.class);

    // Quy tắc nghiệp vụ — chỉnh ở đây nếu muốn đổi
    private static final int MAX_GROUP_NAME = 50;
    private static final int MIN_GROUP_OTHER_MEMBERS = 2;   // không tính người tạo => nhóm tối thiểu 3 người
    private static final int MAX_GROUP_MEMBERS = 100;
    private static final int MIN_SEARCH_LENGTH = 2;
    private static final int MAX_SEARCH_LENGTH = 100;
    private static final int DEFAULT_SEARCH_LIMIT = 10;
    private static final int MAX_SEARCH_LIMIT = 20;
    private static final int MAX_ROOMS_PER_LIST = 200;
    private static final int MAX_MEDIA_URL = 500;
    private static final String IMAGE_PREVIEW = "Đã gửi một ảnh";

    private final MongoDatabase database = MongoConfig.getDatabase();
    private final MongoCollection<Document> messageCollection = database.getCollection("messages");
    private final MongoCollection<Document> roomCollection = database.getCollection("rooms");
    private final MongoCollection<Document> userCollection = database.getCollection("users");
    private final MongoCollection<Document> friendRequestCollection = database.getCollection("friend_requests");
    private final MongoCollection<Document> friendshipCollection = database.getCollection("friendships");

    // Quản lý active subscribers theo user_id cho Realtime Streaming
    private final Map<String, Set<StreamObserver<ChatEvent>>> userSubscribers = new ConcurrentHashMap<>();

    public ChatServiceImpl() {
        Thread t = new Thread(this::ensureIndexes, "mongo-index-init");
        t.setDaemon(true);
        t.start();
    }

    private void ensureIndexes() {
        try {
            messageCollection.createIndex(Indexes.compoundIndex(
                    Indexes.ascending("room_id"),
                    Indexes.descending("timestamp")));
            roomCollection.createIndex(Indexes.compoundIndex(
                    Indexes.ascending("member_ids"),
                    Indexes.descending("updated_at")));
            roomCollection.createIndex(Indexes.ascending("direct_key"),
                    new IndexOptions().unique(true).sparse(true));
            friendRequestCollection.createIndex(Indexes.compoundIndex(
                    Indexes.ascending("receiver_id"),
                    Indexes.ascending("status")));
            friendshipCollection.createIndex(Indexes.compoundIndex(
                            Indexes.ascending("user_id"),
                            Indexes.ascending("friend_id")),
                    new IndexOptions().unique(true));
            log.info("Đã tạo xong index MongoDB");
        } catch (Exception e) {
            log.error("Không tạo được index (sẽ không retry): {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------ Tin nhắn

    @Override
    public void sendMessage(ChatMessage request, StreamObserver<ChatResponse> out) {
        MessageType type = request.getType();
        if (type != MessageType.MESSAGE_TYPE_TEXT && type != MessageType.MESSAGE_TYPE_IMAGE && type != MessageType.MESSAGE_TYPE_VIDEO) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Loại tin nhắn chưa được hỗ trợ").asRuntimeException());
            return;
        }
        boolean isMedia = type == MessageType.MESSAGE_TYPE_IMAGE || type == MessageType.MESSAGE_TYPE_VIDEO;

        if (request.getMessageId().isBlank() || request.getRoomId().isBlank()
                || request.getSenderId().isBlank()) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Thiếu message_id, room_id hoặc sender_id")
                    .asRuntimeException());
            return;
        }
        if (isMedia) {
            String url = request.getMediaUrl();
            if (url.isBlank() || url.length() > MAX_MEDIA_URL || !url.startsWith("https://")) {
                out.onError(Status.INVALID_ARGUMENT
                        .withDescription("media_url không hợp lệ").asRuntimeException());
                return;
            }
        } else if (request.getContent().isBlank()) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Thiếu content").asRuntimeException());
            return;
        }

        try {
            Status denied = checkMembership(request.getRoomId(), request.getSenderId());
            if (denied != null) {
                out.onError(denied.asRuntimeException());
                return;
            }
        } catch (Exception e) {
            log.error("Kiểm tra thành viên phòng {} thất bại", request.getRoomId(), e);
            out.onError(Status.INTERNAL.withDescription("Không gửi được tin nhắn").asRuntimeException());
            return;
        }

        long savedTimestamp = request.getTimestamp();
        boolean inserted = false;
        try {
            String typeStr = type == MessageType.MESSAGE_TYPE_IMAGE ? "IMAGE"
                    : type == MessageType.MESSAGE_TYPE_VIDEO ? "VIDEO" : "TEXT";
            Document message = new Document()
                    .append("_id", request.getMessageId())
                    .append("room_id", request.getRoomId())
                    .append("sender_id", request.getSenderId())
                    .append("content", request.getContent())
                    .append("timestamp", request.getTimestamp())
                    .append("type", typeStr);
            if (isMedia) {
                message.append("media_url", request.getMediaUrl())
                        .append("media_width", request.getMediaWidth())
                        .append("media_height", request.getMediaHeight());
                if (request.getMediaDurationMs() > 0) {
                    message.append("media_duration_ms", request.getMediaDurationMs());
                }
            }
            messageCollection.insertOne(message);
            inserted = true;
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() != ErrorCategory.DUPLICATE_KEY) {
                log.error("Lưu tin nhắn {} thất bại", request.getMessageId(), e);
                out.onError(Status.INTERNAL.withDescription("Không lưu được tin nhắn").asRuntimeException());
                return;
            }
            try {
                Document existing = messageCollection
                        .find(Filters.eq("_id", request.getMessageId())).first();
                if (existing != null && existing.getLong("timestamp") != null) {
                    savedTimestamp = existing.getLong("timestamp");
                }
            } catch (Exception ex) {
                log.warn("Không đọc lại được tin {} sau khi trùng", request.getMessageId(), ex);
            }
        } catch (Exception e) {
            log.error("Lưu tin nhắn {} thất bại", request.getMessageId(), e);
            out.onError(Status.INTERNAL.withDescription("Không lưu được tin nhắn").asRuntimeException());
            return;
        }

        if (inserted) {
            updateLastMessage(request);
            ChatEvent event = ChatEvent.newBuilder()
                    .setEventType(EventType.EVENT_NEW_MESSAGE)
                    .setMessage(request)
                    .setTimestamp(savedTimestamp)
                    .build();
            broadcastToRoom(request.getRoomId(), event);
        }

        out.onNext(ChatResponse.newBuilder()
                .setMessageId(request.getMessageId())
                .setIsSuccess(true)
                .setStatusMessage("Đã lưu tin nhắn")
                .setTimestamp(savedTimestamp)
                .build());
        out.onCompleted();
    }

    /** Cập nhật "tin cuối" của phòng. Lỗi ở đây không làm hỏng việc gửi vì tin đã được lưu. */
    private void updateLastMessage(ChatMessage m) {
        try {
            roomCollection.updateOne(
                    Filters.and(
                            Filters.eq("_id", m.getRoomId()),
                            Filters.or(
                                    Filters.exists("last_message", false),
                                    Filters.lt("last_message.timestamp", m.getTimestamp()))),
                    Updates.combine(
                            Updates.set("last_message", new Document()
                                    .append("message_id", m.getMessageId())
                                    .append("sender_id", m.getSenderId())
                                    .append("content", m.getType() == MessageType.MESSAGE_TYPE_IMAGE
                                            ? IMAGE_PREVIEW
                                            : m.getType() == MessageType.MESSAGE_TYPE_VIDEO
                                            ? "[Video]" : m.getContent())
                                    .append("timestamp", m.getTimestamp())),
                            Updates.max("updated_at", m.getTimestamp())));
        } catch (Exception e) {
            log.warn("Không cập nhật được tin cuối của phòng {}", m.getRoomId(), e);
        }
    }

    @Override
    public void getMessageHistory(HistoryRequest request, StreamObserver<HistoryResponse> responseObserver) {
        if (request.getRoomId().isBlank() || request.getUserId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Thiếu room_id hoặc user_id").asRuntimeException());
            return;
        }

        try {
            Status denied = checkMembership(request.getRoomId(), request.getUserId());
            if (denied != null) {
                responseObserver.onError(denied.asRuntimeException());
                return;
            }

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
                ChatMessage.Builder mb = ChatMessage.newBuilder()
                        .setMessageId(d.getString("_id"))
                        .setRoomId(d.getString("room_id"))
                        .setSenderId(d.getString("sender_id"))
                        .setContent(Objects.toString(d.getString("content"), ""))
                        .setTimestamp(d.getLong("timestamp"));
                // Tin cũ không có trường "type" nên mặc định là text
                if ("IMAGE".equals(d.getString("type"))) {
                    mb.setType(MessageType.MESSAGE_TYPE_IMAGE)
                            .setMediaUrl(Objects.toString(d.getString("media_url"), ""))
                            .setMediaWidth(d.getInteger("media_width", 0))
                            .setMediaHeight(d.getInteger("media_height", 0));
                } else if ("VIDEO".equals(d.getString("type"))) {
                    mb.setType(MessageType.MESSAGE_TYPE_VIDEO)
                            .setMediaUrl(Objects.toString(d.getString("media_url"), ""))
                            .setMediaWidth(d.getInteger("media_width", 0))
                            .setMediaHeight(d.getInteger("media_height", 0))
                            .setMediaDurationMs(d.getLong("media_duration_ms") != null ? d.getLong("media_duration_ms") : 0);
                }
                builder.addMessages(mb);
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Truy vấn lịch sử phòng {} thất bại", request.getRoomId(), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Không lấy được lịch sử tin nhắn").asRuntimeException());
        }
    }

    // ------------------------------------------------------------ Tìm người dùng

    @Override
    public void searchUsers(SearchUsersRequest request, StreamObserver<SearchUsersResponse> out) {
        String requesterId = request.getRequesterId().trim();
        String query = request.getQuery().trim();
        if (requesterId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu requester_id"));
            return;
        }
        if (query.length() < MIN_SEARCH_LENGTH || query.length() > MAX_SEARCH_LENGTH) {
            fail(out, Status.INVALID_ARGUMENT.withDescription(
                    "Từ khóa phải từ " + MIN_SEARCH_LENGTH + " đến " + MAX_SEARCH_LENGTH + " ký tự"));
            return;
        }
        int limit = request.getLimit() <= 0 ? DEFAULT_SEARCH_LIMIT
                : Math.min(request.getLimit(), MAX_SEARCH_LIMIT);

        try {
            Bson match;
            if (query.contains("@")) {
                // Email được lưu đã chuẩn hóa (chữ thường) => khớp theo tiền tố
                match = Filters.regex("account", "^" + Pattern.quote(AuthServiceImpl.normalizeEmail(query)));
            } else {
                // Tên: chứa từ khóa, không phân biệt hoa thường
                match = Filters.regex("display_name", Pattern.quote(query), "i");
            }

            SearchUsersResponse.Builder builder = SearchUsersResponse.newBuilder();
            for (Document u : userCollection
                    .find(Filters.and(match, Filters.ne("_id", requesterId)))
                    .projection(Projections.include("display_name", "account"))
                    .sort(Sorts.ascending("display_name"))
                    .limit(limit)) {
                builder.addUsers(UserSummary.newBuilder()
                        .setUserId(u.getString("_id"))
                        .setDisplayName(u.getString("display_name"))
                        .setEmail(u.getString("account")));
            }
            out.onNext(builder.build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Tìm người dùng thất bại", e);
            fail(out, Status.INTERNAL.withDescription("Không tìm được người dùng"));
        }
    }

    // ------------------------------------------------------------------- Phòng

    @Override
    public void createRoom(CreateRoomRequest request, StreamObserver<Room> out) {
        String creatorId = request.getCreatorId().trim();
        if (creatorId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu creator_id"));
            return;
        }

        Set<String> others = new LinkedHashSet<>();
        for (String id : request.getMemberIdsList()) {
            String trimmed = id.trim();
            if (!trimmed.isEmpty() && !trimmed.equals(creatorId)) others.add(trimmed);
        }

        RoomType type = request.getType();
        String name = request.getName().trim();
        if (type == RoomType.ROOM_TYPE_DIRECT) {
            if (others.size() != 1) {
                fail(out, Status.INVALID_ARGUMENT.withDescription("Phòng 1-1 cần đúng một người còn lại"));
                return;
            }
        } else if (type == RoomType.ROOM_TYPE_GROUP) {
            if (name.isEmpty() || name.length() > MAX_GROUP_NAME) {
                fail(out, Status.INVALID_ARGUMENT.withDescription(
                        "Tên nhóm phải từ 1 đến " + MAX_GROUP_NAME + " ký tự"));
                return;
            }
            if (others.size() < MIN_GROUP_OTHER_MEMBERS) {
                fail(out, Status.INVALID_ARGUMENT.withDescription(
                        "Nhóm cần ít nhất " + (MIN_GROUP_OTHER_MEMBERS + 1) + " thành viên"));
                return;
            }
            if (others.size() + 1 > MAX_GROUP_MEMBERS) {
                fail(out, Status.INVALID_ARGUMENT.withDescription(
                        "Nhóm tối đa " + MAX_GROUP_MEMBERS + " thành viên"));
                return;
            }
        } else {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Loại phòng không hợp lệ"));
            return;
        }

        try {
            List<String> memberIds = new ArrayList<>();
            memberIds.add(creatorId);
            memberIds.addAll(others);

            if (userCollection.countDocuments(Filters.in("_id", memberIds)) != memberIds.size()) {
                fail(out, Status.NOT_FOUND.withDescription("Có người dùng không tồn tại"));
                return;
            }

            Document room = type == RoomType.ROOM_TYPE_DIRECT
                    ? createOrGetDirectRoom(creatorId, memberIds)
                    : createGroupRoom(creatorId, name, memberIds);
            Map<String, String> names = loadDisplayNames(memberIds);

            out.onNext(buildRoom(room, creatorId, names));
            out.onCompleted();
        } catch (Exception e) {
            log.error("Tạo phòng thất bại", e);
            fail(out, Status.INTERNAL.withDescription("Không tạo được phòng"));
        }
    }

    private Document createOrGetDirectRoom(String creatorId, List<String> memberIds) {
        String directKey = memberIds.stream().sorted().collect(Collectors.joining(":"));
        Document existing = roomCollection.find(Filters.eq("direct_key", directKey)).first();
        if (existing != null) return existing;

        long now = System.currentTimeMillis();
        Document room = new Document()
                .append("_id", UUID.randomUUID().toString())
                .append("type", "DIRECT")
                .append("member_ids", memberIds)
                .append("direct_key", directKey)
                .append("created_by", creatorId)
                .append("created_at", now)
                .append("updated_at", now);
        try {
            roomCollection.insertOne(room);
            return room;
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                // Hai người cùng mở phòng một lúc: lấy phòng đã được tạo trước
                return roomCollection.find(Filters.eq("direct_key", directKey)).first();
            }
            throw e;
        }
    }

    private Document createGroupRoom(String creatorId, String name, List<String> memberIds) {
        long now = System.currentTimeMillis();
        Document room = new Document()
                .append("_id", UUID.randomUUID().toString())
                .append("type", "GROUP")
                .append("name", name)
                .append("member_ids", memberIds)
                .append("created_by", creatorId)
                .append("created_at", now)
                .append("updated_at", now);
        roomCollection.insertOne(room);
        return room;
    }

    @Override
    public void listRooms(ListRoomsRequest request, StreamObserver<ListRoomsResponse> out) {
        String userId = request.getUserId().trim();
        if (userId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu user_id"));
            return;
        }
        try {
            List<Document> rooms = roomCollection.find(Filters.eq("member_ids", userId))
                    .sort(Sorts.descending("updated_at"))
                    .limit(MAX_ROOMS_PER_LIST)
                    .into(new ArrayList<>());

            // Gom id mọi thành viên của các phòng để lấy tên một lần
            Set<String> memberIds = new HashSet<>();
            for (Document d : rooms) {
                memberIds.addAll(d.getList("member_ids", String.class));
            }
            Map<String, String> names = loadDisplayNames(memberIds);

            ListRoomsResponse.Builder builder = ListRoomsResponse.newBuilder();
            for (Document d : rooms) {
                builder.addRooms(buildRoom(d, userId, names));
            }
            out.onNext(builder.build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Liệt kê phòng của {} thất bại", userId, e);
            fail(out, Status.INTERNAL.withDescription("Không lấy được danh sách phòng"));
        }
    }

    @Override
    public void markRoomRead(MarkRoomReadRequest request, StreamObserver<MarkRoomReadResponse> out) {
        String userId = request.getUserId().trim();
        String roomId = request.getRoomId().trim();
        if (userId.isEmpty() || roomId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu user_id hoặc room_id"));
            return;
        }
        try {
            Status denied = checkMembership(roomId, userId);
            if (denied != null) {
                fail(out, denied);
                return;
            }

            long readUpTo = request.getReadUpToTimestamp();
            if (readUpTo <= 0) {
                // Không dựa vào đồng hồ của máy nào: lấy mốc thời gian của tin mới nhất trong phòng
                Document latest = messageCollection.find(Filters.eq("room_id", roomId))
                        .sort(Sorts.descending("timestamp"))
                        .projection(Projections.include("timestamp"))
                        .first();
                readUpTo = latest == null ? 0 : latest.getLong("timestamp");
            }
            if (readUpTo > 0) {
                // max => không bao giờ lùi mốc đã đọc
                roomCollection.updateOne(Filters.eq("_id", roomId),
                        Updates.max("last_read." + userId, readUpTo));
            }
            out.onNext(MarkRoomReadResponse.getDefaultInstance());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Đánh dấu đã đọc phòng {} thất bại", roomId, e);
            fail(out, Status.INTERNAL.withDescription("Không đánh dấu được đã đọc"));
        }
    }

    // ----------------------------------------------------------------- Tiện ích

    /** null nếu hợp lệ; ngược lại trả về Status lỗi tương ứng. */
    private Status checkMembership(String roomId, String userId) {
        Document room = roomCollection.find(Filters.eq("_id", roomId))
                .projection(Projections.include("member_ids"))
                .first();
        if (room == null) {
            return Status.NOT_FOUND.withDescription("Phòng không tồn tại");
        }
        List<String> members = room.getList("member_ids", String.class);
        if (members == null || !members.contains(userId)) {
            return Status.PERMISSION_DENIED.withDescription("Bạn không thuộc phòng này");
        }
        return null;
    }

    private Map<String, String> loadDisplayNames(Collection<String> ids) {
        Map<String, String> names = new HashMap<>();
        if (ids.isEmpty()) return names;
        for (Document u : userCollection.find(Filters.in("_id", ids))
                .projection(Projections.include("display_name"))) {
            names.put(u.getString("_id"), u.getString("display_name"));
        }
        return names;
    }

    private Room buildRoom(Document d, String viewerId, Map<String, String> names) {
        String roomId = d.getString("_id");
        List<String> members = d.getList("member_ids", String.class);
        boolean direct = "DIRECT".equals(d.getString("type"));

        String name;
        if (direct) {
            String peerId = members.stream().filter(m -> !m.equals(viewerId)).findFirst().orElse(viewerId);
            name = names.getOrDefault(peerId, "Người dùng");
        } else {
            name = d.getString("name");
        }

        Room.Builder builder = Room.newBuilder()
                .setRoomId(roomId)
                .setType(direct ? RoomType.ROOM_TYPE_DIRECT : RoomType.ROOM_TYPE_GROUP)
                .setName(name == null ? "" : name)
                .addAllMemberIds(members);
        for (String memberId : members) {
            builder.addMembers(RoomMember.newBuilder()
                    .setUserId(memberId)
                    .setDisplayName(names.getOrDefault(memberId, "Người dùng")));
        }

        Document last = d.get("last_message", Document.class);
        if (last != null) {
            builder.setLastMessageContent(last.getString("content"))
                    .setLastMessageSenderId(last.getString("sender_id"))
                    .setLastMessageTimestamp(last.getLong("timestamp"));
        }

        long readTs = 0;
        Document lastRead = d.get("last_read", Document.class);
        if (lastRead != null) {
            Number n = lastRead.get(viewerId, Number.class);
            if (n != null) readTs = n.longValue();
        }
        builder.setUnreadCount(messageCollection.countDocuments(Filters.and(
                Filters.eq("room_id", roomId),
                Filters.gt("timestamp", readTs),
                Filters.ne("sender_id", viewerId))));

        return builder.build();
    }

    private static void fail(StreamObserver<?> out, Status status) {
        out.onError(status.asRuntimeException());
    }

    // ------------------------------------------------------------------ Realtime Streaming

    @Override
    public void subscribeEvents(StreamEventsRequest request, StreamObserver<ChatEvent> responseObserver) {
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
    private void pushToUser(String userId, ChatEvent event) {
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

    private void broadcastToRoom(String roomId, ChatEvent event) {
        try {
            Document room = roomCollection.find(Filters.eq("_id", roomId)).first();
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

    // ------------------------------------------------------------------ Hệ thống kết bạn

    @Override
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
            pushToUser(receiverId, ChatEvent.newBuilder()
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

    @Override
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
                pushToUser(senderId, ChatEvent.newBuilder()
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

    @Override
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

    @Override
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