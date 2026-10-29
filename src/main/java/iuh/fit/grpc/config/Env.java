package iuh.fit.grpc.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class Env {
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    private Env() {}

    public static String get(String key, String defaultValue) {
        String v = dotenv.get(key);
        return (v == null || v.isBlank()) ? defaultValue : v;
    }

    public static String require(String key) {
        String v = dotenv.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Thiếu biến cấu hình " + key);
        }
        return v;
    }
}