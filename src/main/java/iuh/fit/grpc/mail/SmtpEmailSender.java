package iuh.fit.grpc.mail;

import iuh.fit.grpc.config.Env;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import java.util.Properties;

public class SmtpEmailSender implements EmailSender {
    private final Session session;
    private final String from;

    public SmtpEmailSender() {
        String user = Env.require("SMTP_USER");
        String password = Env.require("SMTP_PASSWORD");
        this.from = Env.get("SMTP_FROM", user);

        Properties props = new Properties();
        props.put("mail.smtp.host", Env.get("SMTP_HOST", "smtp.gmail.com"));
        props.put("mail.smtp.port", Env.get("SMTP_PORT", "587"));
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.connectiontimeout", "5000");
        props.put("mail.smtp.timeout", "10000");
        props.put("mail.smtp.writetimeout", "10000");

        this.session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, password);
            }
        });
    }

    @Override
    public void sendOtp(String to, String code, int ttlMinutes, String purpose) throws Exception {
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(from));
        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));
        msg.setSubject(OtpEmailTemplate.subject(purpose), "UTF-8");

        // Bản chữ thuần dự phòng cho ứng dụng thư không hiển thị được HTML
        MimeBodyPart plain = new MimeBodyPart();
        plain.setText(OtpEmailTemplate.text(code, ttlMinutes, purpose), "UTF-8");
        MimeBodyPart html = new MimeBodyPart();
        html.setContent(OtpEmailTemplate.html(code, ttlMinutes, purpose), "text/html; charset=UTF-8");
        MimeMultipart body = new MimeMultipart("alternative");
        body.addBodyPart(plain);
        body.addBodyPart(html);
        msg.setContent(body);
        Transport.send(msg);
    }
}