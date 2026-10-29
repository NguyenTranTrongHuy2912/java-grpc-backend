package iuh.fit.grpc.service;

import at.favre.lib.crypto.bcrypt.BCrypt;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.AuthResponse;
import iuh.fit.grpc.chat.AuthServiceGrpc;
import iuh.fit.grpc.chat.LoginRequest;
import iuh.fit.grpc.chat.RegisterRequest;
import iuh.fit.grpc.chat.RequestOtpRequest;
import iuh.fit.grpc.chat.RequestOtpResponse;
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
            if (users.find(Filters.eq("account", account)).first() != null) {
                out.onError(Status.ALREADY_EXISTS
                        .withDescription("Email đã được đăng ký").asRuntimeException());
                return;
            }
            otpService.send(account);
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
            users.insertOne(new Document()
                    .append("_id", userId)
                    .append("account", account)
                    .append("display_name", displayName)
                    .append("password_hash", hash)
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
}