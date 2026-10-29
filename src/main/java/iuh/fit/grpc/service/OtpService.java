package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import io.grpc.Status;
import iuh.fit.grpc.mail.EmailSender;
import iuh.fit.grpc.config.Env;
import iuh.fit.grpc.config.MongoConfig;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

public class OtpService {
    private static final Logger log = LoggerFactory.getLogger(OtpService.class);

    public static final int TTL_SECONDS = 300;
    public static final int RESEND_SECONDS = 60;
    private static final int MAX_ATTEMPTS = 5;

    private final MongoCollection<Document> otps =
            MongoConfig.getDatabase().getCollection("otp_codes");
    private final SecureRandom random = new SecureRandom();
    private final byte[] secret = Env.require("OTP_SECRET").getBytes(StandardCharsets.UTF_8);
    private final EmailSender emailSender;

    public OtpService(EmailSender emailSender) {
        this.emailSender = emailSender;
        // Mongo tự xóa document khi tới expires_at
        otps.createIndex(Indexes.ascending("expires_at"),
                new IndexOptions().expireAfter(0L, TimeUnit.SECONDS));
    }

    /** Sinh mã, lưu bản băm, gửi email. Ném StatusRuntimeException nếu gửi quá nhanh hoặc gửi lỗi. */
    public void send(String email) {
        long now = System.currentTimeMillis();
        Document old = otps.find(Filters.eq("_id", email)).first();
        if (old != null && now - old.getLong("last_sent_at") < RESEND_SECONDS * 1000L) {
            throw Status.RESOURCE_EXHAUSTED
                    .withDescription("Vui lòng đợi " + RESEND_SECONDS + " giây trước khi gửi lại mã")
                    .asRuntimeException();
        }

        String code = String.format("%06d", random.nextInt(1_000_000));
        otps.replaceOne(Filters.eq("_id", email),
                new Document("_id", email)
                        .append("code_hash", hash(email, code))
                        .append("attempts", 0)
                        .append("last_sent_at", now)
                        .append("expires_at", new Date(now + TTL_SECONDS * 1000L)),
                new ReplaceOptions().upsert(true));

        try {
            emailSender.sendOtp(email, code, TTL_SECONDS / 60);
        } catch (Exception e) {
            log.error("Gửi OTP tới {} thất bại", email, e);
            otps.deleteOne(Filters.eq("_id", email));  // cho phép thử lại ngay
            throw Status.UNAVAILABLE.withDescription("Không gửi được email, thử lại sau")
                    .asRuntimeException();
        }
    }

    /** Đúng thì xóa mã (dùng một lần). Sai hoặc hết hạn thì ném INVALID_ARGUMENT. */
    public void verify(String email, String code) {
        Document doc = otps.findOneAndUpdate(
                Filters.eq("_id", email),
                Updates.inc("attempts", 1),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));

        if (doc == null || doc.getDate("expires_at").getTime() < System.currentTimeMillis()) {
            otps.deleteOne(Filters.eq("_id", email));
            throw Status.INVALID_ARGUMENT
                    .withDescription("Mã không đúng hoặc đã hết hạn").asRuntimeException();
        }
        if (doc.getInteger("attempts") > MAX_ATTEMPTS) {
            otps.deleteOne(Filters.eq("_id", email));
            throw Status.INVALID_ARGUMENT
                    .withDescription("Nhập sai quá nhiều lần, hãy yêu cầu mã mới").asRuntimeException();
        }

        boolean ok = MessageDigest.isEqual(
                doc.getString("code_hash").getBytes(StandardCharsets.UTF_8),
                hash(email, code).getBytes(StandardCharsets.UTF_8));
        if (!ok) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Mã không đúng hoặc đã hết hạn").asRuntimeException();
        }
        otps.deleteOne(Filters.eq("_id", email));
    }

    private String hash(String email, String code) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return HexFormat.of().formatHex(
                    mac.doFinal((email + ":" + code).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}