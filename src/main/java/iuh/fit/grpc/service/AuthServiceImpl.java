package iuh.fit.grpc.service;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.AuthResponse;
import iuh.fit.grpc.chat.AuthServiceGrpc;
import iuh.fit.grpc.chat.ChangePasswordLoggedInRequest;
import iuh.fit.grpc.chat.ChangePasswordLoggedInResponse;
import iuh.fit.grpc.chat.DeleteUserRequest;
import iuh.fit.grpc.chat.DeleteUserResponse;
import iuh.fit.grpc.chat.GetSystemStatsRequest;
import iuh.fit.grpc.chat.GetSystemStatsResponse;
import iuh.fit.grpc.chat.GetUserStatusRequest;
import iuh.fit.grpc.chat.GetUserStatusResponse;
import iuh.fit.grpc.chat.LoginRequest;
import iuh.fit.grpc.chat.OtpPurpose;
import iuh.fit.grpc.chat.RegisterRequest;
import iuh.fit.grpc.chat.RequestOtpRequest;
import iuh.fit.grpc.chat.RequestOtpResponse;
import iuh.fit.grpc.chat.ResetPasswordRequest;
import iuh.fit.grpc.chat.ResetPasswordResponse;
import iuh.fit.grpc.chat.SetUserActiveRequest;
import iuh.fit.grpc.chat.SetUserActiveResponse;
import iuh.fit.grpc.chat.SetUserRoleRequest;
import iuh.fit.grpc.chat.SetUserRoleResponse;
import iuh.fit.grpc.chat.UpdateProfileRequest;
import iuh.fit.grpc.chat.UpdateProfileResponse;
import iuh.fit.grpc.config.Env;
import iuh.fit.grpc.config.MongoConfig;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

public class AuthServiceImpl extends AuthServiceGrpc.AuthServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(AuthServiceImpl.class);
    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final int BCRYPT_COST = 12;
    private static final String DUMMY_HASH =
            BCrypt.withDefaults().hashToString(BCRYPT_COST, "dummy-password".toCharArray());

    // Quy tắc nhập liệu: một nơi duy nhất cho đăng ký, đặt lại và đổi mật khẩu
    private static final int MAX_DISPLAY_NAME = 50;
    private static final int MIN_PASSWORD = 6;
    private static final int MAX_PASSWORD = 64;
    private static final String BAD_LOGIN = "Sai tài khoản hoặc mật khẩu";

    /** Email được cấp quyền ADMIN: biến ADMIN_EMAILS, cách nhau bằng dấu phẩy. Đọc qua Env nên có hiệu lực cả khi chạy bằng file .env. */
    private static final Set<String> ADMIN_EMAILS = parseAdminEmails(Env.get("ADMIN_EMAILS", ""));

    private final MongoCollection<Document> users =
            MongoConfig.getDatabase().getCollection("users");
    private final OtpService otpService;

    public AuthServiceImpl(OtpService otpService) {
        this.otpService = otpService;
        users.createIndex(Indexes.ascending("account"), new IndexOptions().unique(true));
        try {
            // Tự động khởi tạo Role (ADMIN / USER) và trạng thái is_active trên MongoDB từ ADMIN_EMAILS
            if (!ADMIN_EMAILS.isEmpty()) {
                users.updateMany(Filters.in("account", ADMIN_EMAILS), Updates.set("role", "ADMIN"));
            }
            users.updateMany(Filters.exists("role", false), Updates.set("role", "USER"));
            users.updateMany(Filters.exists("is_active", false), Updates.set("is_active", true));
        } catch (Exception e) {
            log.warn("Cập nhật chỉ mục và Role trong MongoDB: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------ Hàm dùng chung

    private static Set<String> parseAdminEmails(String raw) {
        Set<String> emails = new HashSet<>();
        for (String email : raw.split(",")) {
            String clean = email.trim().toLowerCase(Locale.ROOT);
            if (!clean.isEmpty()) emails.add(clean);
        }
        return emails;
    }

    /** Quyết định role khi tạo tài khoản. */
    private static String roleFor(String account) {
        return ADMIN_EMAILS.contains(account) ? "ADMIN" : "USER";
    }

    static String normalizeEmail(String raw) {
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    static boolean isValidEmail(String email) {
        return email.length() <= 254 && EMAIL.matcher(email).matches();
    }

    private static String requireValidEmail(String raw) {
        String account = normalizeEmail(raw);
        if (!isValidEmail(account)) {
            throw Rpc.error(Status.INVALID_ARGUMENT, "Email không hợp lệ");
        }
        return account;
    }

    private static String requirePassword(String password) {
        if (password.length() < MIN_PASSWORD || password.length() > MAX_PASSWORD) {
            throw Rpc.error(Status.INVALID_ARGUMENT,
                    "Mật khẩu phải từ " + MIN_PASSWORD + " đến " + MAX_PASSWORD + " ký tự");
        }
        return password;
    }

    private static String requireOtpCode(String raw) {
        String code = raw.trim();
        if (code.isEmpty()) {
            throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu mã OTP");
        }
        return code;
    }

    /** Lỗi nghiệp vụ của OtpService giữ nguyên; lỗi lạ trả INTERNAL kèm log. */
    private void verifyOtp(String account, String purpose, String code) {
        try {
            otpService.verify(account, purpose, code);
        } catch (StatusRuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.error("Kiểm tra OTP thất bại", e);
            throw Rpc.error(Status.INTERNAL, "Không kiểm tra được mã");
        }
    }

    private static String hashPassword(String password) {
        return BCrypt.withDefaults().hashToString(BCRYPT_COST, password.toCharArray());
    }

    private static Bson passwordUpdate(String hash) {
        return Updates.combine(
                Updates.set("password_hash", hash),
                Updates.set("password_changed_at", System.currentTimeMillis()));
    }

    private static StatusRuntimeException userNotFound() {
        return Rpc.error(Status.NOT_FOUND, "Không tìm thấy người dùng");
    }

    private static AuthResponse toAuthResponse(Document user) {
        AuthResponse.Builder builder = AuthResponse.newBuilder()
                .setUserId(user.getString("_id"))
                .setDisplayName(user.getString("display_name"));
        String avatar = user.getString("avatar_url");
        if (avatar != null) builder.setAvatarUrl(avatar);
        String role = user.getString("role");
        if (role != null) builder.setRole(role);
        return builder.build();
    }

    // ------------------------------------------------------------------ RPC

    @Override
    public void requestOtp(RequestOtpRequest request, StreamObserver<RequestOtpResponse> out) {
        Rpc.respond(out, () -> {
            String account = requireValidEmail(request.getAccount());
            boolean exists = users.find(Filters.eq("account", account)).first() != null;
            if (request.getPurpose() == OtpPurpose.OTP_PURPOSE_RESET_PASSWORD) {
                // Chỉ gửi mail khi tài khoản tồn tại, nhưng luôn trả thành công để không lộ email nào đã đăng ký.
                if (exists) {
                    otpService.send(account, OtpService.PURPOSE_RESET_PASSWORD);
                }
            } else {
                if (exists) {
                    throw Rpc.error(Status.ALREADY_EXISTS, "Email đã được đăng ký");
                }
                otpService.send(account);
            }
            return RequestOtpResponse.newBuilder()
                    .setExpiresInSeconds(OtpService.TTL_SECONDS)
                    .setResendAfterSeconds(OtpService.RESEND_SECONDS)
                    .build();
        }, "Không gửi được mã");
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<AuthResponse> out) {
        Rpc.respond(out, () -> {
            String account = requireValidEmail(request.getAccount());
            String displayName = request.getDisplayName().trim();
            if (displayName.isEmpty() || displayName.length() > MAX_DISPLAY_NAME) {
                throw Rpc.error(Status.INVALID_ARGUMENT,
                        "Tên hiển thị phải từ 1 đến " + MAX_DISPLAY_NAME + " ký tự");
            }
            String password = requirePassword(request.getPassword());
            String otpCode = requireOtpCode(request.getOtpCode());

            verifyOtp(account, OtpService.PURPOSE_REGISTER, otpCode);

            Document user = new Document()
                    .append("_id", UUID.randomUUID().toString())
                    .append("account", account)
                    .append("display_name", displayName)
                    .append("password_hash", hashPassword(password))
                    .append("role", roleFor(account))
                    .append("is_active", true)
                    .append("created_at", System.currentTimeMillis());
            try {
                users.insertOne(user);
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    throw Rpc.error(Status.ALREADY_EXISTS, "Email đã được đăng ký");
                }
                throw e;
            }
            return toAuthResponse(user);
        }, "Không tạo được tài khoản");
    }

    @Override
    public void login(LoginRequest request, StreamObserver<AuthResponse> out) {
        Rpc.respond(out, () -> {
            String account = normalizeEmail(request.getAccount());
            String password = request.getPassword();
            // Chặn sớm mật khẩu quá dài để không tốn CPU băm bcrypt vô ích
            if (account.isEmpty() || password.isEmpty() || password.length() > MAX_PASSWORD) {
                throw Rpc.error(Status.UNAUTHENTICATED, BAD_LOGIN);
            }

            Document user = users.find(Filters.eq("account", account)).first();
            // Luôn băm một lần dù không có tài khoản, để thời gian phản hồi không lộ email nào tồn tại
            String hash = user != null ? user.getString("password_hash") : DUMMY_HASH;
            boolean ok = BCrypt.verifyer().verify(password.toCharArray(), hash).verified;
            if (user == null || !ok) {
                throw Rpc.error(Status.UNAUTHENTICATED, BAD_LOGIN);
            }
            if (user.containsKey("is_active") && !user.getBoolean("is_active", true)) {
                throw Rpc.error(Status.PERMISSION_DENIED, "Tài khoản đã bị tạm khóa bởi Quản trị viên");
            }
            return toAuthResponse(user);
        }, "Không đăng nhập được");
    }

    @Override
    public void resetPassword(ResetPasswordRequest request, StreamObserver<ResetPasswordResponse> out) {
        Rpc.respond(out, () -> {
            String account = requireValidEmail(request.getAccount());
            String password = requirePassword(request.getNewPassword());
            String otpCode = requireOtpCode(request.getOtpCode());

            verifyOtp(account, OtpService.PURPOSE_RESET_PASSWORD, otpCode);

            UpdateResult result = users.updateOne(
                    Filters.eq("account", account), passwordUpdate(hashPassword(password)));
            if (result.getMatchedCount() == 0) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Mã không đúng hoặc đã hết hạn");
            }
            return ResetPasswordResponse.getDefaultInstance();
        }, "Không đổi được mật khẩu");
    }

    @Override
    public void updateProfile(UpdateProfileRequest request, StreamObserver<UpdateProfileResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            if (userId.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu user_id");
            }

            List<Bson> updates = new ArrayList<>();
            if (!request.getDisplayName().isBlank()) {
                updates.add(Updates.set("display_name", request.getDisplayName().trim()));
            }
            if (!request.getAvatarUrl().isBlank()) {
                updates.add(Updates.set("avatar_url", request.getAvatarUrl().trim()));
            }
            if (updates.isEmpty()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Không có thông tin cần cập nhật");
            }

            Document user = users.findOneAndUpdate(
                    Filters.eq("_id", userId),
                    Updates.combine(updates),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            if (user == null) {
                throw userNotFound();
            }
            return UpdateProfileResponse.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Cập nhật thành công")
                    .setDisplayName(user.getString("display_name") != null ? user.getString("display_name") : "")
                    .setAvatarUrl(user.getString("avatar_url") != null ? user.getString("avatar_url") : "")
                    .build();
        }, "Không cập nhật được hồ sơ");
    }

    @Override
    public void changePasswordLoggedIn(ChangePasswordLoggedInRequest request,
                                       StreamObserver<ChangePasswordLoggedInResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            String currentPassword = request.getCurrentPassword();
            String newPassword = request.getNewPassword();
            if (userId.isBlank() || currentPassword.isBlank() || newPassword.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu thông tin mật khẩu");
            }
            requirePassword(newPassword);

            Document user = users.find(Filters.eq("_id", userId)).first();
            if (user == null) {
                throw userNotFound();
            }
            String hash = user.getString("password_hash");
            boolean valid = hash != null
                    && BCrypt.verifyer().verify(currentPassword.toCharArray(), hash.toCharArray()).verified;
            if (!valid) {
                throw Rpc.error(Status.UNAUTHENTICATED, "Mật khẩu hiện tại không đúng");
            }

            users.updateOne(Filters.eq("_id", userId), passwordUpdate(hashPassword(newPassword)));
            return ChangePasswordLoggedInResponse.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Đổi mật khẩu thành công")
                    .build();
        }, "Không đổi được mật khẩu");
    }

    @Override
    public void getUserStatus(GetUserStatusRequest request, StreamObserver<GetUserStatusResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            if (userId == null || userId.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu user_id");
            }
            Document user = users.find(Filters.eq("_id", userId)).first();
            if (user == null) {
                throw userNotFound();
            }
            String role = user.getString("role");
            if (role == null || role.isBlank()) role = "USER";
            boolean isActive = user.getBoolean("is_active", true);
            long pwdChanged = 0L;
            if (user.containsKey("password_changed_at")) {
                Object val = user.get("password_changed_at");
                if (val instanceof Number n) pwdChanged = n.longValue();
            }
            return GetUserStatusResponse.newBuilder()
                    .setUserId(userId)
                    .setRole(role)
                    .setIsActive(isActive)
                    .setPasswordChangedAt(pwdChanged)
                    .build();
        }, "Không lấy được trạng thái người dùng");
    }

    @Override
    public void setUserRole(SetUserRoleRequest request, StreamObserver<SetUserRoleResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            String role = request.getRole();
            if (userId == null || userId.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu user_id");
            }
            String normRole = "ADMIN".equalsIgnoreCase(role) ? "ADMIN" : "USER";
            users.updateOne(Filters.eq("_id", userId), Updates.set("role", normRole));
            return SetUserRoleResponse.newBuilder()
                    .setIsSuccess(true)
                    .setUserId(userId)
                    .setRole(normRole)
                    .build();
        }, "Không cập nhật được quyền người dùng");
    }

    @Override
    public void setUserActive(SetUserActiveRequest request, StreamObserver<SetUserActiveResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            boolean isActive = request.getIsActive();
            if (userId == null || userId.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu user_id");
            }
            users.updateOne(Filters.eq("_id", userId), Updates.set("is_active", isActive));
            return SetUserActiveResponse.newBuilder()
                    .setIsSuccess(true)
                    .setUserId(userId)
                    .setIsActive(isActive)
                    .build();
        }, "Không cập nhật được trạng thái tài khoản");
    }

    @Override
    public void deleteUser(DeleteUserRequest request, StreamObserver<DeleteUserResponse> out) {
        Rpc.respond(out, () -> {
            String userId = request.getUserId();
            if (userId == null || userId.isBlank()) {
                throw Rpc.error(Status.INVALID_ARGUMENT, "Thiếu user_id");
            }
            users.deleteOne(Filters.eq("_id", userId));
            return DeleteUserResponse.newBuilder()
                    .setIsSuccess(true)
                    .setUserId(userId)
                    .build();
        }, "Không xóa được người dùng");
    }

    @Override
    public void getSystemStats(GetSystemStatsRequest request, StreamObserver<GetSystemStatsResponse> out) {
        Rpc.respond(out, () -> {
            long totalUsers = users.countDocuments();
            long totalAdmins = users.countDocuments(Filters.eq("role", "ADMIN"));
            long totalActiveUsers = users.countDocuments(Filters.eq("is_active", true));
            long totalRooms = MongoConfig.getDatabase().getCollection("rooms").countDocuments();
            long totalMessages = MongoConfig.getDatabase().getCollection("messages").countDocuments();
            return GetSystemStatsResponse.newBuilder()
                    .setTotalUsers(totalUsers)
                    .setTotalAdmins(totalAdmins)
                    .setTotalActiveUsers(totalActiveUsers)
                    .setTotalRooms(totalRooms)
                    .setTotalMessages(totalMessages)
                    .build();
        }, "Không lấy được thống kê hệ thống");
    }
}