package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import io.grpc.Status;
import iuh.fit.grpc.chat.Room;
import iuh.fit.grpc.chat.RoomMember;
import iuh.fit.grpc.chat.RoomType;
import org.bson.Document;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Các hàm đọc dùng chung cho nhiều service: kiểm tra thành viên phòng, tên hiển thị, dựng Room cho từng người xem. */
public class RoomSupport {
    private final MongoCollection<Document> rooms;
    private final MongoCollection<Document> users;
    private final MongoCollection<Document> messages;

    public RoomSupport(MongoDatabase db) {
        this.rooms = db.getCollection("rooms");
        this.users = db.getCollection("users");
        this.messages = db.getCollection("messages");
    }

    /** null nếu hợp lệ; ngược lại trả về Status lỗi tương ứng. */
    public Status checkMembership(String roomId, String userId) {
        if ("hq_ai_bot".equals(userId) || "SYSTEM".equals(userId)) {
            return null;
        }
        Document room = rooms.find(Filters.eq("_id", roomId))
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

    public Map<String, String> loadDisplayNames(Collection<String> ids) {
        Map<String, String> names = new HashMap<>();
        if (ids.isEmpty()) return names;
        for (Document u : users.find(Filters.in("_id", ids))
                .projection(Projections.include("display_name"))) {
            names.put(u.getString("_id"), u.getString("display_name"));
        }
        return names;
    }

    public Room buildRoom(Document d, String viewerId, Map<String, String> names) {
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
        builder.setUnreadCount(messages.countDocuments(Filters.and(
                Filters.eq("room_id", roomId),
                Filters.gt("timestamp", readTs),
                Filters.ne("sender_id", viewerId))));

        return builder.build();
    }
}
