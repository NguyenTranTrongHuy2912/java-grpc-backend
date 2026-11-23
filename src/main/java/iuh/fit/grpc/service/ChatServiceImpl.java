package iuh.fit.grpc.service;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import iuh.fit.grpc.chat.AddRoomMembersRequest;
import iuh.fit.grpc.chat.AddRoomMembersResponse;
import iuh.fit.grpc.chat.LeaveRoomRequest;
import iuh.fit.grpc.chat.LeaveRoomResponse;
import iuh.fit.grpc.chat.UpdateRoomNameRequest;
import iuh.fit.grpc.chat.UpdateRoomNameResponse;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import io.grpc.Status;
import static iuh.fit.grpc.service.Rpc.fail;
import static iuh.fit.grpc.service.Rpc.respond;
import iuh.fit.grpc.chat.RevokeMessageRequest;
import iuh.fit.grpc.chat.RevokeMessageResponse;
import iuh.fit.grpc.chat.EditMessageRequest;
import iuh.fit.grpc.chat.EditMessageResponse;
import iuh.fit.grpc.chat.ReactMessageRequest;
import iuh.fit.grpc.chat.ReactMessageResponse;
import iuh.fit.grpc.chat.CreatePollRequest;
import iuh.fit.grpc.chat.VotePollRequest;
import iuh.fit.grpc.chat.PollResponse;
import iuh.fit.grpc.chat.PinMessageRequest;
import iuh.fit.grpc.chat.PinMessageResponse;
import iuh.fit.grpc.chat.UnpinMessageRequest;
import iuh.fit.grpc.chat.UnpinMessageResponse;
import iuh.fit.grpc.chat.ToggleStarMessageRequest;
import iuh.fit.grpc.chat.ToggleStarMessageResponse;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.ChatMessage;
import iuh.fit.grpc.chat.ChatResponse;
import iuh.fit.grpc.chat.ChatServiceGrpc;
import iuh.fit.grpc.chat.CreateRoomRequest;
import iuh.fit.grpc.chat.HistoryRequest;
import iuh.fit.grpc.chat.ChatEvent;
import iuh.fit.grpc.chat.EventType;
import iuh.fit.grpc.chat.StreamEventsRequest;
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
import iuh.fit.grpc.chat.RoomType;
import iuh.fit.grpc.chat.SearchUsersRequest;
import iuh.fit.grpc.chat.SearchUsersResponse;
import iuh.fit.grpc.config.MongoConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ChatServiceImpl.class);

    // Quy tắc nghiệp vụ — chỉnh ở đây nếu muốn đổi
    private static final int MIN_GROUP_OTHER_MEMBERS = 2;   // không tính người tạo => nhóm tối thiểu 3 người
    private static final int MAX_GROUP_MEMBERS = 100;
    private static final int MAX_ROOMS_PER_LIST = 200;
    private static final int MAX_MEDIA_URL = 500;
    private static final String IMAGE_PREVIEW = "Đã gửi một ảnh";

    private final MongoDatabase database = MongoConfig.getDatabase();
    private final MongoCollection<Document> messageCollection = database.getCollection("messages");
    private final MongoCollection<Document> roomCollection = database.getCollection("rooms");


    // Thu hồi, sửa, cảm xúc, bình chọn, ghim, đánh dấu sao: logic nằm ở MessageActions
    // Hạ tầng dùng chung: realtime stream và các hàm đọc phòng. Các service tách ra sau này nhận lại hai đối tượng này.
    private final RealtimeHub hub = new RealtimeHub(database);
    private final RoomSupport support = new RoomSupport(database);
    private final FriendService friends = new FriendService(database, hub);
    private final UserSearchService userSearch = new UserSearchService(database);

    private final MessageActions actions =
            new MessageActions(database, hub::broadcastToRoom, this::updateLastMessage);

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
            friends.ensureIndexes();
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
            Status denied = support.checkMembership(request.getRoomId(), request.getSenderId());
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
            hub.broadcastToRoom(request.getRoomId(), event);
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
            Status denied = support.checkMembership(request.getRoomId(), request.getUserId());
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

    // ------------------------------------------------------------ Hành động trên tin nhắn

    @Override
    public void revokeMessage(RevokeMessageRequest request, StreamObserver<RevokeMessageResponse> out) {
        respond(out, () -> actions.revoke(request), "Không thu hồi được tin nhắn");
    }

    @Override
    public void editMessage(EditMessageRequest request, StreamObserver<EditMessageResponse> out) {
        respond(out, () -> actions.edit(request), "Không sửa được tin nhắn");
    }

    @Override
    public void reactMessage(ReactMessageRequest request, StreamObserver<ReactMessageResponse> out) {
        respond(out, () -> actions.react(request), "Không thả được cảm xúc");
    }

    @Override
    public void createPoll(CreatePollRequest request, StreamObserver<PollResponse> out) {
        respond(out, () -> actions.createPoll(request), "Không tạo được cuộc bình chọn");
    }

    @Override
    public void votePoll(VotePollRequest request, StreamObserver<PollResponse> out) {
        respond(out, () -> actions.votePoll(request), "Không ghi nhận được bình chọn");
    }

    @Override
    public void pinMessage(PinMessageRequest request, StreamObserver<PinMessageResponse> out) {
        respond(out, () -> actions.pin(request), "Không ghim được tin nhắn");
    }

    @Override
    public void unpinMessage(UnpinMessageRequest request, StreamObserver<UnpinMessageResponse> out) {
        respond(out, () -> actions.unpin(request), "Không bỏ ghim được tin nhắn");
    }

    @Override
    public void toggleStarMessage(ToggleStarMessageRequest request, StreamObserver<ToggleStarMessageResponse> out) {
        respond(out, () -> actions.toggleStar(request), "Không lưu được tin nhắn");
    }

    // ------------------------------------------------------------ Tìm người dùng

    @Override
    public void searchUsers(SearchUsersRequest request, StreamObserver<SearchUsersResponse> out) {
        userSearch.searchUsers(request, out);
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
            Status badName = RoomSupport.checkGroupName(name);
            if (badName != null) {
                fail(out, badName);
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

            Status missing = support.checkUsersExist(memberIds);
            if (missing != null) {
                fail(out, missing);
                return;
            }

            Document room = type == RoomType.ROOM_TYPE_DIRECT
                    ? createOrGetDirectRoom(creatorId, memberIds)
                    : createGroupRoom(creatorId, name, memberIds);
            Map<String, String> names = support.loadDisplayNames(memberIds);

            out.onNext(support.buildRoom(room, creatorId, names));
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
            Map<String, String> names = support.loadDisplayNames(memberIds);

            ListRoomsResponse.Builder builder = ListRoomsResponse.newBuilder();
            for (Document d : rooms) {
                builder.addRooms(support.buildRoom(d, userId, names));
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
            Status denied = support.checkMembership(roomId, userId);
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

                ChatEvent event = ChatEvent.newBuilder()
                        .setEventType(EventType.EVENT_READ_RECEIPT)
                        .setUserId(userId)
                        .setTimestamp(readUpTo)
                        .setRoom(Room.newBuilder().setRoomId(roomId).build())
                        .build();
                hub.broadcastToRoom(roomId, event);
            }
            out.onNext(MarkRoomReadResponse.getDefaultInstance());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Đánh dấu đã đọc phòng {} thất bại", roomId, e);
            fail(out, Status.INTERNAL.withDescription("Không đánh dấu được đã đọc"));
        }
    }

    // ------------------------------------------------------------------ Quản lý nhóm

    /**
     * Rời nhóm. Gọi lại khi đã rời thì vẫn thành công (idempotent).
     * Nếu người rời là trưởng nhóm, quyền chuyển cho thành viên (không phải bot) đứng đầu danh sách còn lại.
     */
    @Override
    public void leaveRoom(LeaveRoomRequest request, StreamObserver<LeaveRoomResponse> out) {
        String userId = request.getUserId().trim();
        String roomId = request.getRoomId().trim();
        if (userId.isEmpty() || roomId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu user_id hoặc room_id"));
            return;
        }
        try {
            Document room = roomCollection.find(Filters.eq("_id", roomId)).first();
            if (room == null) {
                fail(out, Status.NOT_FOUND.withDescription("Không tìm thấy phòng chat."));
                return;
            }
            if (!"GROUP".equals(room.getString("type"))) {
                fail(out, Status.INVALID_ARGUMENT.withDescription("Chỉ có thể rời phòng chat nhóm"));
                return;
            }

            // Gỡ khỏi danh sách chỉ khi còn là thành viên: hai yêu cầu rời cùng lúc chỉ một yêu cầu thắng
            Document after = roomCollection.findOneAndUpdate(
                    Filters.and(Filters.eq("_id", roomId), Filters.eq("member_ids", userId)),
                    Updates.combine(
                            Updates.pull("member_ids", userId),
                            Updates.unset("last_read." + userId)),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            if (after == null) {
                out.onNext(LeaveRoomResponse.newBuilder()
                        .setIsSuccess(true)
                        .setStatusMessage("Bạn không còn trong phòng chat này.")
                        .build());
                out.onCompleted();
                return;
            }

            List<String> remaining = after.getList("member_ids", String.class);
            if (remaining == null) remaining = List.of();
            if (userId.equals(after.getString("created_by"))) {
                remaining.stream()
                        .filter(m -> !Ids.AI_BOT.equals(m))
                        .findFirst()
                        .ifPresent(newOwner -> roomCollection.updateOne(
                                Filters.eq("_id", roomId), Updates.set("created_by", newOwner)));
            }

            String name = support.loadDisplayNames(List.of(userId)).getOrDefault(userId, "Một thành viên");
            postSystemMessage(roomId, name + " đã rời khỏi nhóm.");
            // Người vừa rời không còn trong danh sách thành viên nên cần báo riêng để app bỏ phòng khỏi danh sách
            hub.pushToUser(userId, RealtimeHub.roomUpdatedEvent(roomId));

            out.onNext(LeaveRoomResponse.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Bạn đã rời khỏi nhóm thành công.")
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Rời nhóm {} thất bại", roomId, e);
            fail(out, Status.INTERNAL.withDescription("Không rời được nhóm"));
        }
    }

    /** Thêm thành viên vào nhóm. Mọi thành viên của nhóm đều được thêm người (như Messenger). */
    @Override
    public void addRoomMembers(AddRoomMembersRequest request, StreamObserver<AddRoomMembersResponse> out) {
        String userId = request.getUserId().trim();
        String roomId = request.getRoomId().trim();
        if (userId.isEmpty() || roomId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu user_id hoặc room_id"));
            return;
        }
        Set<String> requested = new LinkedHashSet<>();
        for (String id : request.getMemberIdsList()) {
            String trimmed = id.trim();
            if (!trimmed.isEmpty() && !Ids.SYSTEM.equals(trimmed)) requested.add(trimmed);
        }
        if (requested.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Chưa chọn thành viên để thêm"));
            return;
        }

        try {
            Document room = roomCollection.find(Filters.eq("_id", roomId)).first();
            Status denied = requireGroupMember(room, userId);
            if (denied != null) {
                fail(out, denied);
                return;
            }

            List<String> current = room.getList("member_ids", String.class);
            Set<String> toAdd = new LinkedHashSet<>(requested);
            toAdd.removeAll(current);

            if (!toAdd.isEmpty()) {
                if (current.size() + toAdd.size() > MAX_GROUP_MEMBERS) {
                    fail(out, Status.INVALID_ARGUMENT.withDescription(
                            "Nhóm tối đa " + MAX_GROUP_MEMBERS + " thành viên"));
                    return;
                }
                Status missing = support.checkUsersExist(toAdd);
                if (missing != null) {
                    fail(out, missing);
                    return;
                }
                roomCollection.updateOne(Filters.eq("_id", roomId),
                        Updates.addEachToSet("member_ids", new ArrayList<>(toAdd)));

                Map<String, String> names = support.loadDisplayNames(
                        Stream.concat(Stream.of(userId), toAdd.stream()).collect(Collectors.toList()));
                String addedNames = toAdd.stream()
                        .map(id -> names.getOrDefault(id, "Người dùng"))
                        .collect(Collectors.joining(", "));
                postSystemMessage(roomId, names.getOrDefault(userId, "Một thành viên")
                        + " đã thêm " + addedNames + " vào nhóm.");
            }

            Document updated = roomCollection.find(Filters.eq("_id", roomId)).first();
            out.onNext(AddRoomMembersResponse.newBuilder()
                    .setRoom(support.buildRoom(updated, userId, support.loadDisplayNames(updated.getList("member_ids", String.class))))
                    .setAddedCount(toAdd.size())
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Thêm thành viên vào phòng {} thất bại", roomId, e);
            fail(out, Status.INTERNAL.withDescription("Không thêm được thành viên"));
        }
    }

    /** Đổi tên nhóm. Mọi thành viên của nhóm đều được đổi tên. */
    @Override
    public void updateRoomName(UpdateRoomNameRequest request, StreamObserver<UpdateRoomNameResponse> out) {
        String userId = request.getUserId().trim();
        String roomId = request.getRoomId().trim();
        String name = request.getName().trim();
        if (userId.isEmpty() || roomId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu user_id hoặc room_id"));
            return;
        }
        Status badName = RoomSupport.checkGroupName(name);
        if (badName != null) {
            fail(out, badName);
            return;
        }

        try {
            Document room = roomCollection.find(Filters.eq("_id", roomId)).first();
            Status denied = requireGroupMember(room, userId);
            if (denied != null) {
                fail(out, denied);
                return;
            }

            Document current = room;
            if (!name.equals(room.getString("name"))) {
                roomCollection.updateOne(Filters.eq("_id", roomId), Updates.set("name", name));
                String actor = support.loadDisplayNames(List.of(userId)).getOrDefault(userId, "Một thành viên");
                postSystemMessage(roomId, actor + " đã đổi tên nhóm thành \"" + name + "\".");
                current = roomCollection.find(Filters.eq("_id", roomId)).first();
            }

            out.onNext(UpdateRoomNameResponse.newBuilder()
                    .setRoom(support.buildRoom(current, userId, support.loadDisplayNames(current.getList("member_ids", String.class))))
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Đổi tên phòng {} thất bại", roomId, e);
            fail(out, Status.INTERNAL.withDescription("Không đổi được tên nhóm"));
        }
    }

    /** null nếu hợp lệ: phòng tồn tại, là nhóm, và người thực hiện đang là thành viên. */
    private Status requireGroupMember(Document room, String userId) {
        if (room == null) {
            return Status.NOT_FOUND.withDescription("Không tìm thấy phòng chat.");
        }
        if (!"GROUP".equals(room.getString("type"))) {
            return Status.INVALID_ARGUMENT.withDescription("Chỉ áp dụng cho phòng chat nhóm");
        }
        List<String> members = room.getList("member_ids", String.class);
        if (members == null || !members.contains(userId)) {
            return Status.PERMISSION_DENIED.withDescription("Bạn không thuộc phòng này");
        }
        return null;
    }

    /** Lưu tin nhắn hệ thống, cập nhật tin cuối của phòng rồi phát realtime tới các thành viên hiện tại. */
    private void postSystemMessage(String roomId, String content) {
        long now = System.currentTimeMillis();
        String id = UUID.randomUUID().toString();
        try {
            messageCollection.insertOne(new Document()
                    .append("_id", id)
                    .append("room_id", roomId)
                    .append("sender_id", Ids.SYSTEM)
                    .append("content", content)
                    .append("timestamp", now)
                    .append("type", "TEXT"));
        } catch (Exception e) {
            log.warn("Không lưu được tin hệ thống của phòng {}", roomId, e);
            return;
        }
        ChatMessage msg = ChatMessage.newBuilder()
                .setMessageId(id)
                .setRoomId(roomId)
                .setSenderId(Ids.SYSTEM)
                .setContent(content)
                .setTimestamp(now)
                .setType(MessageType.MESSAGE_TYPE_TEXT)
                .build();
        updateLastMessage(msg);
        hub.broadcastToRoom(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_NEW_MESSAGE)
                .setMessage(msg)
                .setTimestamp(now)
                .build());
        hub.broadcastToRoom(roomId, RealtimeHub.roomUpdatedEvent(roomId));
    }

    // ------------------------------------------------------------------ Realtime Streaming

    @Override
    public void subscribeEvents(StreamEventsRequest request, StreamObserver<ChatEvent> responseObserver) {
        hub.subscribe(request, responseObserver);
    }

    // ------------------------------------------------------------------ Hệ thống kết bạn

    @Override
    public void sendFriendRequest(SendFriendRequestReq request, StreamObserver<SendFriendRequestResp> out) {
        friends.sendFriendRequest(request, out);
    }

    @Override
    public void respondFriendRequest(RespondFriendRequestReq request, StreamObserver<RespondFriendRequestResp> out) {
        friends.respondFriendRequest(request, out);
    }

    @Override
    public void listFriends(ListFriendsReq request, StreamObserver<ListFriendsResp> out) {
        friends.listFriends(request, out);
    }

    @Override
    public void listFriendRequests(ListFriendRequestsReq request, StreamObserver<ListFriendRequestsResp> out) {
        friends.listFriendRequests(request, out);
    }
}