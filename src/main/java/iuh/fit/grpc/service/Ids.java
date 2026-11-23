package iuh.fit.grpc.service;

/** Định danh nội bộ không phải người dùng thật. Khớp với gateway và client. */
final class Ids {
    /** Người gửi của tin nhắn hệ thống (rời nhóm, thêm thành viên, đổi tên...). */
    static final String SYSTEM = "system";
    static final String AI_BOT = "hq_ai_bot";

    private Ids() {}

    /** Tin hệ thống và bot AI không cần là thành viên phòng. Vẫn chấp nhận "SYSTEM" dạng cũ. */
    static boolean isInternalActor(String id) {
        return AI_BOT.equals(id) || SYSTEM.equalsIgnoreCase(id);
    }
}
