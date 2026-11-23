package iuh.fit.grpc.mail;

public interface EmailSender {
    /** Mục đích của mã OTP. Đây là nơi định nghĩa duy nhất; OtpService và các nơi khác dùng lại. */
    String PURPOSE_REGISTER = "register";
    String PURPOSE_RESET_PASSWORD = "reset_password";

    /** purpose: PURPOSE_REGISTER hoặc PURPOSE_RESET_PASSWORD */
    void sendOtp(String to, String code, int ttlMinutes, String purpose) throws Exception;
}