import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class AuthManager {

    // Temporary password for our Redis project
    private static final String PASSWORD = "redis123";

    private AuthManager() {
    }

    // =========================================================
    // AUTHENTICATE
    // =========================================================

    public static boolean authenticate(String password) {

        if (password == null) {
            return false;
        }

        byte[] expected =
                PASSWORD.getBytes(
                        StandardCharsets.UTF_8
                );

        byte[] actual =
                password.getBytes(
                        StandardCharsets.UTF_8
                );

        return MessageDigest.isEqual(
                expected,
                actual
        );
    }


    // =========================================================
    // CHECK AUTH STATUS
    // =========================================================

    public static boolean isAuthenticationEnabled() {

        return PASSWORD != null
                && !PASSWORD.isEmpty();
    }
}