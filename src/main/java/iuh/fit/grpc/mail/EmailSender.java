package iuh.fit.grpc.mail;

public interface EmailSender {
    /** purpose: OtpService.PURPOSE_REGISTER hoặc OtpService.PURPOSE_RESET_PASSWORD */
    void sendOtp(String to, String code, int ttlMinutes, String purpose) throws Exception;
}
