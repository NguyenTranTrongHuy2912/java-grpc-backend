package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import io.grpc.Status;
import iuh.fit.grpc.chat.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Các hành động trên tin nhắn: thu hồi, sửa, thả cảm xúc, bình chọn, ghim, đánh dấu sao.
 * Đây là nơi DUY NHẤT ghi các dữ liệu này; gateway chỉ gọi RPC.
 * <p>
 * Mọi hàm public ném StatusRuntimeException khi lỗi nghiệp vụ. ChatServiceImpl chuyển lỗi đó
 * sang StreamObserver, còn lỗi khác (Mongo...) thành INTERNAL.
 * <p>
 * Schema Mongo giữ nguyên như gateway cũ đang ghi, để các chỗ gateway còn đọc trực tiếp
 * (lịch sử, danh sách tin đã lưu...) tiếp tục chạy.
 */
public class MessageActions {
    private static final Logger log = LoggerFactory.getLogger(MessageActions.class);

    private static final long EDIT_WINDOW_MS = 30 * 60 * 1000L;
    private static final int MAX_CONTENT = 5000;        // khớp EditMessageBody ở gateway
    private static final int MAX_QUESTION = 500;
    private static final int MAX_OPTION_TEXT = 500;
    private static final int MIN_POLL_OPTIONS = 2;
    private static final int MAX_POLL_OPTIONS = 10;
    private static final int MAX_EMOJI_LENGTH = 16;
    private static final int MAX_CAS_RETRIES = 4;
    private static final String REVOKED_TEXT = "Tin nhắn đã được thu hồi";
    private static final String DEFAULT_NAME = "Thành viên";

    private final MongoCollection<Document> messages;
    private final MongoCollection<Document> rooms;
    private final MongoCollection<Document> users;
    private final MongoCollection<Document> polls;
    private final MongoCollection<Document> starred;
    private final BiConsumer<String, ChatEvent> roomBroadcaster;
    private final Consumer<ChatMessage> lastMessageUpdater;

    public MessageActions(MongoDatabase db,
                          BiConsumer<String, ChatEvent> roomBroadcaster,
                          Consumer<ChatMessage> lastMessageUpdater) {
        this.messages = db.getCollection("messages");
        this.rooms = db.getCollection("rooms");
        this.users = db.getCollection("users");
        this.polls = db.getCollection("polls");
        this.starred = db.getCollection("starred_messages");
        this.roomBroadcaster = roomBroadcaster;
        this.lastMessageUpdater = lastMessageUpdater;
    }

    // ------------------------------------------------------------------ Thu hồi

    public RevokeMessageResponse revoke(RevokeMessageRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId(), messageId = r.getMessageId();
        require(!userId.isBlank() && !roomId.isBlank() && !messageId.isBlank(),
                "Thiếu user_id, room_id hoặc message_id");
        requireMember(roomId, userId);

        Document msg = findMessage(roomId, messageId, "Không tìm thấy tin nhắn cần thu hồi.");
        if (!userId.equals(msg.getString("sender_id"))) {
            throw Status.PERMISSION_DENIED
                    .withDescription("Bạn chỉ có thể thu hồi tin nhắn do chính mình gửi.").asRuntimeException();
        }
        if (isRevoked(msg)) {
            return alreadyRevoked(messageId);
        }
        long now = System.currentTimeMillis();
        requireWithinWindow(msg, now, "thu hồi");

        // Điều kiện ne(is_revoked, true) để hai yêu cầu đồng thời không ghi đè nhau
        UpdateResult res = messages.updateOne(
                Filters.and(Filters.eq("_id", messageId), Filters.ne("is_revoked", true)),
                Updates.combine(
                        Updates.set("content", REVOKED_TEXT),
                        Updates.set("is_revoked", true),
                        Updates.set("revoked_at", now)));
        if (res.getModifiedCount() == 0) {
            return alreadyRevoked(messageId);
        }

        refreshLastMessage(roomId, messageId, REVOKED_TEXT);
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_MESSAGE_REVOKED)
                .setTimestamp(now)
                .setMessageUpdate(MessageUpdate.newBuilder()
                        .setRoomId(roomId).setMessageId(messageId).setSenderId(userId)
                        .setContent(REVOKED_TEXT).setTimestamp(now))
                .build());

        return RevokeMessageResponse.newBuilder()
                .setMessageId(messageId).setContent(REVOKED_TEXT).build();
    }

    private static RevokeMessageResponse alreadyRevoked(String messageId) {
        return RevokeMessageResponse.newBuilder()
                .setMessageId(messageId).setContent(REVOKED_TEXT).setAlreadyRevoked(true).build();
    }

    // ------------------------------------------------------------------ Sửa

    public EditMessageResponse edit(EditMessageRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId(), messageId = r.getMessageId();
        String content = r.getContent().strip();
        require(!userId.isBlank() && !roomId.isBlank() && !messageId.isBlank(),
                "Thiếu user_id, room_id hoặc message_id");
        require(!content.isEmpty(), "Nội dung tin nhắn không được để trống.");
        require(content.length() <= MAX_CONTENT, "Tin nhắn tối đa " + MAX_CONTENT + " ký tự.");
        requireMember(roomId, userId);

        Document msg = findMessage(roomId, messageId, "Không tìm thấy tin nhắn cần chỉnh sửa.");
        if (!userId.equals(msg.getString("sender_id"))) {
            throw Status.PERMISSION_DENIED
                    .withDescription("Bạn chỉ có thể chỉnh sửa tin nhắn do chính mình gửi.").asRuntimeException();
        }
        if (isRevoked(msg)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Không thể chỉnh sửa tin nhắn đã thu hồi.").asRuntimeException();
        }
        String type = msg.getString("type");
        if (type != null && !"TEXT".equalsIgnoreCase(type)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Chỉ có thể chỉnh sửa tin nhắn văn bản.").asRuntimeException();
        }
        long now = System.currentTimeMillis();
        requireWithinWindow(msg, now, "chỉnh sửa");

        UpdateResult res = messages.updateOne(
                Filters.and(Filters.eq("_id", messageId), Filters.ne("is_revoked", true)),
                Updates.combine(
                        Updates.set("content", content),
                        Updates.set("is_edited", true),
                        Updates.set("edited_at", now)));
        if (res.getMatchedCount() == 0) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Không thể chỉnh sửa tin nhắn đã thu hồi.").asRuntimeException();
        }

        refreshLastMessage(roomId, messageId, content);
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_MESSAGE_EDITED)
                .setTimestamp(now)
                .setMessageUpdate(MessageUpdate.newBuilder()
                        .setRoomId(roomId).setMessageId(messageId).setSenderId(userId)
                        .setContent(content).setIsEdited(true).setEditedAt(now))
                .build());

        return EditMessageResponse.newBuilder()
                .setMessageId(messageId).setContent(content).setEditedAt(now).build();
    }

    // ------------------------------------------------------------------ Cảm xúc

    public ReactMessageResponse react(ReactMessageRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId(), messageId = r.getMessageId();
        String emoji = r.getEmoji().strip();
        require(!userId.isBlank() && !roomId.isBlank() && !messageId.isBlank(),
                "Thiếu user_id, room_id hoặc message_id");
        // emoji được dùng làm tên trường trong Mongo nên không cho chứa ký tự đặc biệt của đường dẫn
        require(!emoji.isEmpty() && emoji.length() <= MAX_EMOJI_LENGTH
                        && emoji.indexOf('.') < 0 && emoji.indexOf('$') < 0 && emoji.indexOf('\0') < 0,
                "Emoji không hợp lệ.");
        requireMember(roomId, userId);

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Document msg = findMessage(roomId, messageId, "Không tìm thấy tin nhắn.");
            Document old = msg.get("reactions", Document.class);
            Map<String, List<String>> next = toReactionMap(old);
            toggleReaction(next, userId, emoji);

            // Chỉ ghi nếu reactions chưa bị ai đổi từ lúc đọc; nếu có thì đọc lại và tính lại
            Bson filter = Filters.and(Filters.eq("_id", messageId), Filters.eq("reactions", old));
            UpdateResult res = messages.updateOne(filter, Updates.set("reactions", toReactionDocument(next)));
            if (res.getMatchedCount() > 0) {
                List<ReactionEntry> entries = toEntries(next);
                publish(roomId, ChatEvent.newBuilder()
                        .setEventType(EventType.EVENT_MESSAGE_REACTION)
                        .setTimestamp(System.currentTimeMillis())
                        .setMessageUpdate(MessageUpdate.newBuilder()
                                .setRoomId(roomId).setMessageId(messageId)
                                .addAllReactions(entries).setUserId(userId).setEmoji(emoji))
                        .build());
                return ReactMessageResponse.newBuilder()
                        .setMessageId(messageId).addAllReactions(entries).build();
            }
        }
        throw Status.ABORTED.withDescription("Nhiều người đang thao tác cùng lúc, hãy thử lại.").asRuntimeException();
    }

    /** Bấm lại cùng emoji thì gỡ; bấm emoji khác thì chuyển sang emoji mới (mỗi người một cảm xúc). */
    private static void toggleReaction(Map<String, List<String>> reactions, String userId, String emoji) {
        List<String> current = reactions.get(emoji);
        if (current != null && current.contains(userId)) {
            current.remove(userId);
            if (current.isEmpty()) reactions.remove(emoji);
            return;
        }
        for (Iterator<Map.Entry<String, List<String>>> it = reactions.entrySet().iterator(); it.hasNext(); ) {
            List<String> ids = it.next().getValue();
            ids.remove(userId);
            if (ids.isEmpty()) it.remove();
        }
        reactions.computeIfAbsent(emoji, k -> new ArrayList<>()).add(userId);
    }

    private static Map<String, List<String>> toReactionMap(Document d) {
        Map<String, List<String>> map = new LinkedHashMap<>();
        if (d == null) return map;
        for (Map.Entry<String, Object> e : d.entrySet()) {
            List<String> ids = new ArrayList<>();
            if (e.getValue() instanceof List<?> list) {
                for (Object o : list) if (o != null) ids.add(o.toString());
            }
            if (!ids.isEmpty()) map.put(e.getKey(), ids);
        }
        return map;
    }

    private static Document toReactionDocument(Map<String, List<String>> map) {
        Document d = new Document();
        map.forEach(d::append);
        return d;
    }

    private static List<ReactionEntry> toEntries(Map<String, List<String>> map) {
        List<ReactionEntry> out = new ArrayList<>();
        map.forEach((emoji, ids) -> out.add(
                ReactionEntry.newBuilder().setEmoji(emoji).addAllUserIds(ids).build()));
        return out;
    }

    // ------------------------------------------------------------------ Bình chọn

    public PollResponse createPoll(CreatePollRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId();
        String question = r.getQuestion().strip();
        require(!userId.isBlank() && !roomId.isBlank(), "Thiếu user_id hoặc room_id");
        require(!question.isEmpty() && question.length() <= MAX_QUESTION,
                "Câu hỏi phải từ 1 đến " + MAX_QUESTION + " ký tự.");

        List<String> options = new ArrayList<>();
        for (String o : r.getOptionsList()) {
            String t = o.strip();
            if (!t.isEmpty()) options.add(t);
        }
        require(options.size() >= MIN_POLL_OPTIONS, "Cuộc bình chọn cần ít nhất 2 lựa chọn.");
        require(options.size() <= MAX_POLL_OPTIONS, "Tối đa 10 lựa chọn cho một cuộc bình chọn.");
        for (String o : options) {
            require(o.length() <= MAX_OPTION_TEXT, "Mỗi lựa chọn tối đa " + MAX_OPTION_TEXT + " ký tự.");
        }
        requireMember(roomId, userId);

        long now = System.currentTimeMillis();
        String pollId = "poll_" + shortId(12);
        String messageId = "msg_" + shortId(16);
        String content = "📊 [Bình chọn] " + question;

        List<Document> optionDocs = new ArrayList<>();
        for (int i = 0; i < options.size(); i++) {
            optionDocs.add(new Document("id", "opt_" + i)
                    .append("text", options.get(i))
                    .append("voter_ids", new ArrayList<String>()));
        }
        Document pollDoc = new Document("_id", pollId)
                .append("room_id", roomId)
                .append("message_id", messageId)
                .append("creator_id", userId)
                .append("creator_name", displayName(userId))
                .append("question", question)
                .append("options", optionDocs)
                .append("is_multiple_choice", r.getIsMultipleChoice())
                .append("is_anonymous", r.getIsAnonymous())
                .append("is_closed", false)
                .append("created_at", now);

        // Ghi poll trước, rồi mới ghi tin nhắn tham chiếu nó; tin nhắn hỏng thì gỡ poll
        polls.insertOne(pollDoc);
        try {
            messages.insertOne(new Document("_id", messageId)
                    .append("room_id", roomId)
                    .append("sender_id", userId)
                    .append("content", content)
                    .append("timestamp", now)
                    .append("type", "POLL")
                    .append("poll_id", pollId));
        } catch (RuntimeException e) {
            try {
                polls.deleteOne(Filters.eq("_id", pollId));
            } catch (Exception ignored) {
                log.warn("Không gỡ được poll {} sau khi lưu tin nhắn thất bại", pollId);
            }
            throw e;
        }

        ChatMessage chatMessage = ChatMessage.newBuilder()
                .setMessageId(messageId).setRoomId(roomId).setSenderId(userId)
                .setContent(content).setTimestamp(now)
                .setType(MessageType.MESSAGE_TYPE_TEXT).build();
        lastMessageUpdater.accept(chatMessage);
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_NEW_MESSAGE)
                .setMessage(chatMessage).setTimestamp(now).build());

        Poll poll = toPoll(pollDoc);
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_POLL_CREATED)
                .setTimestamp(now).setPoll(poll).build());
        return PollResponse.newBuilder().setPoll(poll).build();
    }

    public PollResponse votePoll(VotePollRequest r) {
        String userId = r.getUserId(), pollId = r.getPollId(), optionId = r.getOptionId().strip();
        require(!userId.isBlank() && !pollId.isBlank() && !optionId.isEmpty(),
                "Thiếu user_id, poll_id hoặc option_id");

        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Document poll = polls.find(Filters.eq("_id", pollId)).first();
            if (poll == null) {
                throw Status.NOT_FOUND.withDescription("Không tìm thấy cuộc bình chọn.").asRuntimeException();
            }
            String roomId = poll.getString("room_id");
            requireMember(roomId, userId);
            if (poll.getBoolean("is_closed", false)) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("Cuộc bình chọn này đã kết thúc.").asRuntimeException();
            }

            List<Document> oldOptions = poll.getList("options", Document.class, new ArrayList<>());
            boolean valid = false;
            for (Document o : oldOptions) {
                if (optionId.equals(o.getString("id"))) valid = true;
            }
            require(valid, "Lựa chọn không hợp lệ.");

            boolean multiple = poll.getBoolean("is_multiple_choice", false);
            List<Document> newOptions = new ArrayList<>();
            for (Document o : oldOptions) {
                List<String> voters = new ArrayList<>(o.getList("voter_ids", String.class, new ArrayList<>()));
                if (optionId.equals(o.getString("id"))) {
                    if (!voters.remove(userId)) voters.add(userId);   // bấm lại = bỏ phiếu
                } else if (!multiple) {
                    voters.remove(userId);                            // chọn một: chuyển phiếu sang lựa chọn mới
                }
                newOptions.add(new Document(o).append("voter_ids", voters));
            }

            UpdateResult res = polls.updateOne(
                    Filters.and(Filters.eq("_id", pollId), Filters.eq("options", oldOptions)),
                    Updates.set("options", newOptions));
            if (res.getMatchedCount() > 0) {
                poll.put("options", newOptions);
                Poll proto = toPoll(poll);
                publish(roomId, ChatEvent.newBuilder()
                        .setEventType(EventType.EVENT_POLL_UPDATED)
                        .setTimestamp(System.currentTimeMillis()).setPoll(proto).build());
                return PollResponse.newBuilder().setPoll(proto).build();
            }
        }
        throw Status.ABORTED.withDescription("Nhiều người đang bình chọn cùng lúc, hãy thử lại.").asRuntimeException();
    }

    private static Poll toPoll(Document d) {
        Poll.Builder b = Poll.newBuilder()
                .setPollId(d.getString("_id"))
                .setRoomId(Objects.toString(d.getString("room_id"), ""))
                .setMessageId(Objects.toString(d.getString("message_id"), ""))
                .setCreatorId(Objects.toString(d.getString("creator_id"), ""))
                .setCreatorName(Objects.toString(d.getString("creator_name"), DEFAULT_NAME))
                .setQuestion(Objects.toString(d.getString("question"), ""))
                .setIsMultipleChoice(d.getBoolean("is_multiple_choice", false))
                .setIsAnonymous(d.getBoolean("is_anonymous", false))
                .setIsClosed(d.getBoolean("is_closed", false))
                .setCreatedAt(longOf(d, "created_at"));
        for (Document o : d.getList("options", Document.class, new ArrayList<>())) {
            b.addOptions(PollOption.newBuilder()
                    .setOptionId(Objects.toString(o.getString("id"), ""))
                    .setText(Objects.toString(o.getString("text"), ""))
                    .addAllVoterIds(o.getList("voter_ids", String.class, new ArrayList<>())));
        }
        return b.build();
    }

    // ------------------------------------------------------------------ Ghim

    public PinMessageResponse pin(PinMessageRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId(), messageId = r.getMessageId();
        require(!userId.isBlank() && !roomId.isBlank() && !messageId.isBlank(),
                "Thiếu user_id, room_id hoặc message_id");
        requireMember(roomId, userId);
        Document msg = findMessage(roomId, messageId, "Không tìm thấy tin nhắn cần ghim.");

        long now = System.currentTimeMillis();
        String senderId = Objects.toString(msg.getString("sender_id"), "");
        Document pinned = new Document("id", messageId)
                .append("content", Objects.toString(msg.getString("content"), ""))
                .append("sender_id", senderId)
                .append("sender_name", displayName(senderId))
                .append("timestamp", msg.get("timestamp", Number.class) != null ? longOf(msg, "timestamp") : now)
                .append("pinned_by", userId)
                .append("pinned_at", now);
        rooms.updateOne(Filters.eq("_id", roomId), Updates.set("pinned_message", pinned));

        PinnedMessage proto = toPinned(pinned);
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_ROOM_PINNED).setTimestamp(now)
                .setPinUpdate(PinUpdate.newBuilder().setRoomId(roomId).setPinnedMessage(proto))
                .build());
        return PinMessageResponse.newBuilder().setPinnedMessage(proto).build();
    }

    public UnpinMessageResponse unpin(UnpinMessageRequest r) {
        String userId = r.getUserId(), roomId = r.getRoomId();
        require(!userId.isBlank() && !roomId.isBlank(), "Thiếu user_id hoặc room_id");
        requireMember(roomId, userId);

        rooms.updateOne(Filters.eq("_id", roomId), Updates.unset("pinned_message"));
        publish(roomId, ChatEvent.newBuilder()
                .setEventType(EventType.EVENT_ROOM_PINNED)
                .setTimestamp(System.currentTimeMillis())
                .setPinUpdate(PinUpdate.newBuilder().setRoomId(roomId))
                .build());
        return UnpinMessageResponse.getDefaultInstance();
    }

    private static PinnedMessage toPinned(Document d) {
        return PinnedMessage.newBuilder()
                .setMessageId(Objects.toString(d.getString("id"), ""))
                .setContent(Objects.toString(d.getString("content"), ""))
                .setSenderId(Objects.toString(d.getString("sender_id"), ""))
                .setSenderName(Objects.toString(d.getString("sender_name"), DEFAULT_NAME))
                .setTimestamp(longOf(d, "timestamp"))
                .setPinnedBy(Objects.toString(d.getString("pinned_by"), ""))
                .setPinnedAt(longOf(d, "pinned_at"))
                .build();
    }

    // ------------------------------------------------------------------ Đánh dấu sao

    public ToggleStarMessageResponse toggleStar(ToggleStarMessageRequest r) {
        String userId = r.getUserId(), messageId = r.getMessageId();
        require(!userId.isBlank() && !messageId.isBlank(), "Thiếu user_id hoặc message_id");

        // Đã lưu thì bỏ lưu (không cần còn là thành viên phòng mới được bỏ dấu sao của chính mình)
        long removed = starred.deleteOne(Filters.and(
                Filters.eq("user_id", userId), Filters.eq("message_id", messageId))).getDeletedCount();
        if (removed > 0) {
            return ToggleStarMessageResponse.newBuilder().setMessageId(messageId).setStarred(false).build();
        }

        Document msg = messages.find(Filters.eq("_id", messageId)).first();
        if (msg == null) {
            throw Status.NOT_FOUND.withDescription("Không tìm thấy tin nhắn cần lưu.").asRuntimeException();
        }
        String roomId = Objects.toString(msg.getString("room_id"), "");
        requireMember(roomId, userId);   // không cho lưu (và đọc lại) tin của phòng mình không thuộc

        long now = System.currentTimeMillis();
        String senderId = Objects.toString(msg.getString("sender_id"), "");
        String id = userId + "_" + messageId;
        Document doc = new Document("_id", id)
                .append("user_id", userId)
                .append("message_id", messageId)
                .append("room_id", roomId)
                .append("content", Objects.toString(msg.getString("content"), ""))
                .append("sender_id", senderId)
                .append("sender_name", displayName(senderId))
                .append("type", Objects.toString(msg.getString("type"), "TEXT"))
                .append("media_url", msg.get("media_url"))
                .append("file_name", msg.get("file_name"))
                .append("file_size", msg.get("file_size"))
                .append("timestamp", msg.get("timestamp", Number.class) != null ? longOf(msg, "timestamp") : now)
                .append("starred_at", now);
        starred.replaceOne(Filters.eq("_id", id), doc, new ReplaceOptions().upsert(true));
        return ToggleStarMessageResponse.newBuilder().setMessageId(messageId).setStarred(true).build();
    }

    // ------------------------------------------------------------------ Tiện ích

    private static void require(boolean ok, String message) {
        if (!ok) throw Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }

    /** Ném NOT_FOUND / PERMISSION_DENIED nếu phòng không có hoặc user không thuộc phòng. */
    private Document requireMember(String roomId, String userId) {
        Document room = rooms.find(Filters.eq("_id", roomId))
                .projection(Projections.include("member_ids")).first();
        if (room == null) {
            throw Status.NOT_FOUND.withDescription("Phòng không tồn tại").asRuntimeException();
        }
        List<String> members = room.getList("member_ids", String.class);
        if (members == null || !members.contains(userId)) {
            throw Status.PERMISSION_DENIED.withDescription("Bạn không thuộc phòng này").asRuntimeException();
        }
        return room;
    }

    private Document findMessage(String roomId, String messageId, String notFound) {
        Document msg = messages.find(Filters.and(
                Filters.eq("_id", messageId), Filters.eq("room_id", roomId))).first();
        if (msg == null) {
            throw Status.NOT_FOUND.withDescription(notFound).asRuntimeException();
        }
        return msg;
    }

    private static boolean isRevoked(Document msg) {
        return Boolean.TRUE.equals(msg.getBoolean("is_revoked"));
    }

    private static void requireWithinWindow(Document msg, long now, String action) {
        Number ts = msg.get("timestamp", Number.class);
        if (ts != null && now - ts.longValue() > EDIT_WINDOW_MS) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Đã quá 30 phút kể từ lúc gửi. Không thể " + action + " tin nhắn này nữa.")
                    .asRuntimeException();
        }
    }

    /**
     * Nếu tin vừa đổi đang là "tin cuối" của phòng thì cập nhật nội dung hiển thị ở danh sách phòng.
     * Khớp theo message_id trong last_message (cùng cấu trúc ChatServiceImpl.updateLastMessage ghi).
     */
    private void refreshLastMessage(String roomId, String messageId, String newContent) {
        try {
            UpdateResult res = rooms.updateOne(
                    Filters.and(Filters.eq("_id", roomId), Filters.eq("last_message.message_id", messageId)),
                    Updates.set("last_message.content", newContent));
            if (res.getModifiedCount() > 0) {
                publish(roomId, ChatEvent.newBuilder()
                        .setEventType(EventType.EVENT_ROOM_UPDATED)
                        .setRoom(Room.newBuilder().setRoomId(roomId))
                        .setTimestamp(System.currentTimeMillis())
                        .build());
            }
        } catch (Exception e) {
            log.warn("Không cập nhật được tin cuối của phòng {}", roomId, e);
        }
    }

    private String displayName(String userId) {
        if (userId == null || userId.isBlank()) return DEFAULT_NAME;
        Document u = users.find(Filters.eq("_id", userId))
                .projection(Projections.include("display_name")).first();
        String name = u == null ? null : u.getString("display_name");
        return (name == null || name.isBlank()) ? DEFAULT_NAME : name;
    }

    private void publish(String roomId, ChatEvent event) {
        roomBroadcaster.accept(roomId, event);
    }

    private static long longOf(Document d, String key) {
        Number n = d.get(key, Number.class);
        return n == null ? 0L : n.longValue();
    }

    private static String shortId(int length) {
        return UUID.randomUUID().toString().replace("-", "").substring(0, length);
    }
}
