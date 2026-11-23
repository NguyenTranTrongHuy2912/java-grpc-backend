package iuh.fit.grpc.mail;

import iuh.fit.grpc.config.Env;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Gửi OTP qua HTTP API của Brevo (cổng 443), dùng khi nơi deploy chặn SMTP (vd Render gói miễn phí).
 * Biến môi trường: BREVO_API_KEY (bắt buộc), MAIL_FROM_EMAIL (bắt buộc, phải là email đã xác minh
 * trong Brevo), MAIL_FROM_NAME (tùy chọn).
 */
public class HttpEmailSender implements EmailSender {
    private static final URI ENDPOINT = URI.create("https://api.brevo.com/v3/smtp/email");

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final String apiKey;
    private final String fromEmail;
    private final String fromName;

    public HttpEmailSender() {
        this.apiKey = Env.require("BREVO_API_KEY");
        this.fromEmail = Env.require("MAIL_FROM_EMAIL");
        this.fromName = Env.get("MAIL_FROM_NAME", "HQ ChatApp");
    }

    @Override
    public void sendOtp(String to, String code, int ttlMinutes, String purpose) throws Exception {
        String json = "{"
                + "\"sender\":{\"name\":" + q(fromName) + ",\"email\":" + q(fromEmail) + "},"
                + "\"to\":[{\"email\":" + q(to) + "}],"
                + "\"subject\":" + q(OtpEmailTemplate.subject(purpose)) + ","
                + "\"htmlContent\":" + q(OtpEmailTemplate.html(code, ttlMinutes, purpose)) + ","
                + "\"textContent\":" + q(OtpEmailTemplate.text(code, ttlMinutes, purpose))
                + "}";

        HttpRequest req = HttpRequest.newBuilder(ENDPOINT)
                .timeout(Duration.ofSeconds(10))
                .header("api-key", apiKey)
                .header("accept", "application/json")
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            // Không log OTP, chỉ log mã trạng thái và phản hồi của Brevo
            throw new IllegalStateException("Brevo trả về " + res.statusCode() + ": " + res.body());
        }
    }

    /** Bọc chuỗi thành JSON string hợp lệ (escape ký tự điều khiển, dấu nháy, xuống dòng). */
    private static String q(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        return sb.append('"').toString();
    }
}
