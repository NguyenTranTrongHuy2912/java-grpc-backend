package iuh.fit.grpc.service;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.AuthResponse;
import iuh.fit.grpc.chat.AuthServiceGrpc;
import iuh.fit.grpc.chat.LoginRequest;
import iuh.fit.grpc.chat.OtpPurpose;
import iuh.fit.grpc.chat.RegisterRequest;
import iuh.fit.grpc.chat.RequestOtpRequest;
import iuh.fit.grpc.chat.RequestOtpResponse;
import iuh.fit.grpc.chat.ResetPasswordRequest;
import iuh.fit.grpc.chat.ResetPasswordResponse;
import iuh.fit.grpc.chat.UpdateProfileRequest;
import iuh.fit.grpc.chat.UpdateProfileResponse;
import iuh.fit.grpc.chat.ChangePasswordLoggedInRequest;
import iuh.fit.grpc.chat.ChangePasswordLoggedInResponse;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import java.util.ArrayList;
import java.util.List;
import org.bson.conversions.Bson;

import iuh.fit.grpc.config.MongoConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

public class AuthServiceImpl extends AuthServiceGrpc.AuthServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(AuthServiceImpl.class);
    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final int BCRYPT_COST = 12;
    private static final String DUMMY_HASH =
            BCrypt.withDefaults().hashToString(BCRYPT_COST, "dummy-password".toCharArray());

    private final MongoCollection<Document> users =
            MongoConfig.getDatabase().getCollection("users");
    private final OtpService otpService;

    public AuthServiceImpl(OtpService otpService) {
        this.otpService = otpService;
        users.createIndex(Indexes.ascending("account"), new IndexOptions().unique(true));
        try {
            // Tự động khởi tạo Role (ADMIN / USER) và trạng thái is_active trên MongoDB
            users.updateMany(
                Filters.in("account", "quoc05@gmail.com", "admin@chatapp.com", "admin@gmail.com", "admin@iuh.edu.vn"),
                Updates.set("role", "ADMIN")
            );
            users.updateMany(
                Filters.exists("role", false),
                Updates.set("role", "USER")
            );
            users.updateMany(
                Filters.exists("is_active", false),
                Updates.set("is_active", true)
            );
        } catch (Exception e) {
            log.warn("Cập nhật chỉ mục và Role trong MongoDB: {}", e.getMessage());
        }
    }

    static String normalizeEmail(String raw) {
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    static boolean isValidEmail(String email) {
        return email.length() <= 254 && EMAIL.matcher(email).matches();
    }

    @Override
    public void requestOtp(RequestOtpRequest request, StreamObserver<RequestOtpResponse> out) {
        String account = normalizeEmail(request.getAccount());
        if (!isValidEmail(account)) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Email không hợp lệ").asRuntimeException());
            return;
        }
        try {
            boolean exists = users.find(Filters.eq("account", account)).first() != null;
            if (request.getPurpose() == OtpPurpose.OTP_PURPOSE_RESET_PASSWORD) {
                // Chỉ gửi mail khi tài khoản tồn tại, nhưng luôn trả thành công
                // để không lộ email nào đã đăng ký.
                if (exists) {
                    otpService.send(account, OtpService.PURPOSE_RESET_PASSWORD);
                }
            } else {
                if (exists) {
                    out.onError(Status.ALREADY_EXISTS
                            .withDescription("Email đã được đăng ký").asRuntimeException());
                    return;
                }
                otpService.send(account);
            }
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        } catch (Exception e) {
            log.error("Yêu cầu OTP thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không gửi được mã").asRuntimeException());
            return;
        }
        out.onNext(RequestOtpResponse.newBuilder()
                .setExpiresInSeconds(OtpService.TTL_SECONDS)
                .setResendAfterSeconds(OtpService.RESEND_SECONDS)
                .build());
        out.onCompleted();
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<AuthResponse> out) {
        String account = normalizeEmail(request.getAccount());
        String displayName = request.getDisplayName().trim();
        String password = request.getPassword();
        String otpCode = request.getOtpCode().trim();

        if (!isValidEmail(account)) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Email không hợp lệ").asRuntimeException());
            return;
        }
        if (displayName.isEmpty() || displayName.length() > 50) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Tên hiển thị phải từ 1 đến 50 ký tự").asRuntimeException());
            return;
        }
        if (password.length() < 6 || password.length() > 64) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Mật khẩu phải từ 6 đến 64 ký tự").asRuntimeException());
            return;
        }
        if (otpCode.isEmpty()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu mã OTP").asRuntimeException());
            return;
        }

        try {
            otpService.verify(account, otpCode);
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        } catch (Exception e) {
            log.error("Kiểm tra OTP thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không kiểm tra được mã").asRuntimeException());
            return;
        }

        String userId = UUID.randomUUID().toString();
        try {
            String hash = BCrypt.withDefaults().hashToString(BCRYPT_COST, password.toCharArray());
            String role = (account.equalsIgnoreCase("quoc05@gmail.com")
                    || account.equalsIgnoreCase("admin@chatapp.com")
                    || account.equalsIgnoreCase("admin@gmail.com")
                    || account.equalsIgnoreCase("admin@iuh.edu.vn")) ? "ADMIN" : "USER";

            users.insertOne(new Document()
                    .append("_id", userId)
                    .append("account", account)
                    .append("display_name", displayName)
                    .append("password_hash", hash)
                    .append("role", role)
                    .append("is_active", true)
                    .append("created_at", System.currentTimeMillis()));
        } catch (MongoWriteException e) {
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                out.onError(Status.ALREADY_EXISTS
                        .withDescription("Email đã được đăng ký").asRuntimeException());
            } else {
                log.error("Đăng ký tài khoản thất bại", e);
                out.onError(Status.INTERNAL.withDescription("Không tạo được tài khoản").asRuntimeException());
            }
            return;
        } catch (Exception e) {
            log.error("Đăng ký tài khoản thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không tạo được tài khoản").asRuntimeException());
            return;
        }

        out.onNext(AuthResponse.newBuilder().setUserId(userId).setDisplayName(displayName).build());
        out.onCompleted();
    }

    @Override
    public void login(LoginRequest request, StreamObserver<AuthResponse> out) {
        String account = normalizeEmail(request.getAccount());
        String password = request.getPassword();

        if (account.isEmpty() || password.isEmpty() || password.length() > 64) {
            out.onError(Status.UNAUTHENTICATED
                    .withDescription("Sai tài khoản hoặc mật khẩu").asRuntimeException());
            return;
        }

        try {
            Document user = users.find(Filters.eq("account", account)).first();
            String hash = user != null ? user.getString("password_hash") : DUMMY_HASH;
            boolean ok = BCrypt.verifyer().verify(password.toCharArray(), hash).verified;

            if (user == null || !ok) {
                out.onError(Status.UNAUTHENTICATED
                        .withDescription("Sai tài khoản hoặc mật khẩu").asRuntimeException());
                return;
            }

            if (user.containsKey("is_active") && !user.getBoolean("is_active", true)) {
                out.onError(Status.PERMISSION_DENIED
                        .withDescription("Tài khoản đã bị tạm khóa bởi Quản trị viên").asRuntimeException());
                return;
            }

            out.onNext(AuthResponse.newBuilder()
                    .setUserId(user.getString("_id"))
                    .setDisplayName(user.getString("display_name"))
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Đăng nhập thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không đăng nhập được").asRuntimeException());
        }
    }

    @Override
    public void resetPassword(ResetPasswordRequest request, StreamObserver<ResetPasswordResponse> out) {
        String account = normalizeEmail(request.getAccount());
        String password = request.getNewPassword();
        String otpCode = request.getOtpCode().trim();

        if (!isValidEmail(account)) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Email không hợp lệ").asRuntimeException());
            return;
        }
        if (password.length() < 6 || password.length() > 64) {
            out.onError(Status.INVALID_ARGUMENT
                    .withDescription("Mật khẩu phải từ 6 đến 64 ký tự").asRuntimeException());
            return;
        }
        if (otpCode.isEmpty()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu mã OTP").asRuntimeException());
            return;
        }

        try {
            otpService.verify(account, OtpService.PURPOSE_RESET_PASSWORD, otpCode);
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        } catch (Exception e) {
            log.error("Kiểm tra OTP thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không kiểm tra được mã").asRuntimeException());
            return;
        }

        try {
            String hash = BCrypt.withDefaults().hashToString(BCRYPT_COST, password.toCharArray());
            UpdateResult result = users.updateOne(
                    Filters.eq("account", account),
                    Updates.combine(
                            Updates.set("password_hash", hash),
                            Updates.set("password_changed_at", System.currentTimeMillis())));
            if (result.getMatchedCount() == 0) {
                out.onError(Status.INVALID_ARGUMENT
                        .withDescription("Mã không đúng hoặc đã hết hạn").asRuntimeException());
                return;
            }
        } catch (Exception e) {
            log.error("Đặt lại mật khẩu thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không đổi được mật khẩu").asRuntimeException());
            return;
        }

        out.onNext(ResetPasswordResponse.getDefaultInstance());
        out.onCompleted();
    }

    @Override
    public void updateProfile(UpdateProfileRequest request, StreamObserver<UpdateProfileResponse> out) {
        String userId = request.getUserId();
        if (userId.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu user_id").asRuntimeException());
            return;
        }

        List<Bson> updates = new ArrayList<>();
        if (!request.getDisplayName().isBlank()) {
            updates.add(Updates.set("display_name", request.getDisplayName().trim()));
        }
        if (!request.getAvatarUrl().isBlank()) {
            updates.add(Updates.set("avatar_url", request.getAvatarUrl().trim()));
        }

        if (updates.isEmpty()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Không có thông tin cần cập nhật").asRuntimeException());
            return;
        }

        try {
            Document user = users.findOneAndUpdate(
                    Filters.eq("_id", userId),
                    Updates.combine(updates),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)
            );
            if (user == null) {
                out.onError(Status.NOT_FOUND.withDescription("Không tìm thấy người dùng").asRuntimeException());
                return;
            }

            out.onNext(UpdateProfileResponse.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Cập nhật thành công")
                    .setDisplayName(user.getString("display_name") != null ? user.getString("display_name") : "")
                    .setAvatarUrl(user.getString("avatar_url") != null ? user.getString("avatar_url") : "")
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Cập nhật profile thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không cập nhật được hồ sơ").asRuntimeException());
        }
    }

    @Override
    public void changePasswordLoggedIn(ChangePasswordLoggedInRequest request, StreamObserver<ChangePasswordLoggedInResponse> out) {
        String userId = request.getUserId();
        String currentPassword = request.getCurrentPassword();
        String newPassword = request.getNewPassword();

        if (userId.isBlank() || currentPassword.isBlank() || newPassword.isBlank()) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Thiếu thông tin mật khẩu").asRuntimeException());
            return;
        }

        if (newPassword.length() < 6) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Mật khẩu mới phải từ 6 ký tự trở lên").asRuntimeException());
            return;
        }

        try {
            Document user = users.find(Filters.eq("_id", userId)).first();
            if (user == null) {
                out.onError(Status.NOT_FOUND.withDescription("Không tìm thấy người dùng").asRuntimeException());
                return;
            }

            String hash = user.getString("password_hash");
            boolean valid = hash != null && BCrypt.verifyer().verify(currentPassword.toCharArray(), hash.toCharArray()).verified;
            if (!valid) {
                out.onError(Status.UNAUTHENTICATED.withDescription("Mật khẩu hiện tại không đúng").asRuntimeException());
                return;
            }

            String newHash = BCrypt.withDefaults().hashToString(BCRYPT_COST, newPassword.toCharArray());
            users.updateOne(
                    Filters.eq("_id", userId),
                    Updates.combine(
                            Updates.set("password_hash", newHash),
                            Updates.set("password_changed_at", System.currentTimeMillis())
                    )
            );

            out.onNext(ChangePasswordLoggedInResponse.newBuilder()
                    .setIsSuccess(true)
                    .setStatusMessage("Đổi mật khẩu thành công")
                    .build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Đổi mật khẩu thất bại", e);
            out.onError(Status.INTERNAL.withDescription("Không đổi được mật khẩu").asRuntimeException());
        }
    }
}
