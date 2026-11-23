package iuh.fit.grpc.mail;

import iuh.fit.grpc.config.Env;

/**
 * Nội dung email OTP (HTML và bản chữ thuần dự phòng).
 * Chỉ chèn mã số và thời hạn, không chèn dữ liệu do người dùng nhập nên không cần escape HTML.
 * Logo lấy từ biến môi trường MAIL_LOGO_URL (đường dẫn HTTPS công khai). Để trống thì không hiện logo.
 */
public final class OtpEmailTemplate {
    private OtpEmailTemplate() {}

    private static boolean isReset(String purpose) {
        return EmailSender.PURPOSE_RESET_PASSWORD.equals(purpose);
    }

    public static String subject(String purpose) {
        return isReset(purpose) ? "HQ ChatApp - Đặt lại mật khẩu" : "HQ ChatApp - Mã xác thực đăng ký tài khoản";
    }

    public static String text(String code, int ttlMinutes, String purpose) {
        String intro = isReset(purpose)
                ? "Bạn vừa yêu cầu đặt lại mật khẩu. Mã xác thực của bạn là: "
                : "Mã xác thực đăng ký tài khoản của bạn là: ";
        String ignore = isReset(purpose)
                ? "Nếu bạn không yêu cầu đặt lại mật khẩu, hãy bỏ qua email này, mật khẩu của bạn vẫn an toàn."
                : "Nếu bạn không yêu cầu, hãy bỏ qua email này.";
        return intro + code + "\n\nMã có hiệu lực trong " + ttlMinutes + " phút. " + ignore;
    }

    public static String html(String code, int ttlMinutes, String purpose) {
        String desc = isReset(purpose)
                ? "Bạn vừa yêu cầu đặt lại mật khẩu cho tài khoản của mình."
                : "Bạn (hoặc Quản trị viên) vừa yêu cầu tạo tài khoản trên hệ thống <strong>HQ ChatApp</strong>.";
        return PAGE
                .replace("{{LOGO}}", logoHtml())
                .replace("{{DESC}}", desc)
                .replace("{{CODE}}", code)
                .replace("{{TTL}}", String.valueOf(ttlMinutes));
    }

    /** Thẻ img cho logo; trả về chuỗi rỗng nếu chưa cấu hình MAIL_LOGO_URL. */
    private static String logoHtml() {
        String url = Env.get("MAIL_LOGO_URL", "").trim();
        if (url.isEmpty()) return "";
        return "<img src=\"" + url + "\" width=\"72\" height=\"72\" alt=\"HQ ChatApp\" "
                + "style=\"display: block; margin: 0 auto 14px; border-radius: 18px; background-color: #ffffff;\">";
    }

    private static final String PAGE = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
            <body style="margin: 0; padding: 0; background-color: #f4f6f9; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;">
                <table border="0" cellpadding="0" cellspacing="0" width="100%" style="table-layout: fixed;">
                    <tr>
                        <td align="center" style="padding: 40px 10px;">
                            <table border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width: 520px; background-color: #ffffff; border-radius: 16px; overflow: hidden; border: 1px solid #e2e8f0;">
                                <tr>
                                    <td align="center" style="padding: 32px 24px 20px; background: #2563EB;">
                                        {{LOGO}}
                                        <h1 style="color: #ffffff; font-size: 22px; font-weight: 800; margin: 0 0 4px;">HQ ChatApp Security</h1>
                                        <p style="color: #bfdbfe; font-size: 13px; margin: 0; font-weight: 500;">Hệ thống xác thực tài khoản an toàn</p>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 32px 28px 24px;">
                                        <p style="font-size: 15px; color: #1e293b; line-height: 24px; margin: 0 0 12px;">Xin chào bạn,</p>
                                        <p style="font-size: 14px; color: #475569; line-height: 22px; margin: 0 0 24px;">
                                            {{DESC}} Dưới đây là mã xác thực OTP dùng một lần của bạn:
                                        </p>
                                        <div style="background-color: #f8fafc; border: 2px dashed #cbd5e1; border-radius: 12px; padding: 20px; text-align: center; margin-bottom: 24px;">
                                            <div style="font-size: 32px; font-weight: 800; letter-spacing: 8px; color: #2563EB; font-family: monospace;">{{CODE}}</div>
                                            <p style="font-size: 12px; color: #64748b; margin: 8px 0 0;">(Mã có hiệu lực trong vòng <strong>{{TTL}} phút</strong>)</p>
                                        </div>
                                        <p style="font-size: 13px; color: #64748b; line-height: 20px; margin: 0 0 16px;">
                                            <strong>Lưu ý bảo mật:</strong> Tuyệt đối không cung cấp mã xác thực này cho bất kỳ ai. Nhân viên hỗ trợ sẽ không bao giờ yêu cầu mã OTP của bạn.
                                        </p>
                                        <p style="font-size: 13px; color: #94a3b8; line-height: 20px; margin: 0;">
                                            Nếu bạn không thực hiện yêu cầu này, vui lòng bỏ qua email này.
                                        </p>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 20px 28px; background-color: #f8fafc; border-top: 1px solid #f1f5f9; text-align: center;">
                                        <p style="font-size: 12px; color: #94a3b8; margin: 0;">© 2026 HQ ChatApp Messenger. Toàn quyền bảo lưu.</p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
            </html>
            """;
}