import java.security.SecureRandom;

public final class ReplicationId {

    // Redis-style replication ID length
    private static final int ID_LENGTH = 40;

    private static final SecureRandom RANDOM =
            new SecureRandom();

    private static final char[] HEX =
            "0123456789abcdef".toCharArray();

    private ReplicationId() {
    }

    // =========================================================
    // GENERATE NEW REPLICATION ID
    // =========================================================

    public static String generate() {

        byte[] bytes =
                new byte[ID_LENGTH / 2];

        RANDOM.nextBytes(
                bytes
        );

        char[] result =
                new char[ID_LENGTH];

        int index =
                0;

        for (byte value : bytes) {

            int unsigned =
                    value & 0xFF;

            result[index++] =
                    HEX[
                            unsigned >>> 4
                            ];

            result[index++] =
                    HEX[
                            unsigned & 0x0F
                            ];
        }

        return new String(
                result
        );
    }

    // =========================================================
    // VALIDATE
    // =========================================================

    public static boolean isValid(
            String replicationId
    ) {

        if (
                replicationId == null
                        ||
                replicationId.length()
                        !=
                ID_LENGTH
        ) {

            return false;
        }

        for (
                int i = 0;
                i < replicationId.length();
                i++
        ) {

            char c =
                    Character.toLowerCase(
                            replicationId.charAt(i)
                    );

            boolean digit =
                    c >= '0'
                            &&
                    c <= '9';

            boolean hexLetter =
                    c >= 'a'
                            &&
                    c <= 'f';

            if (
                    !digit
                            &&
                    !hexLetter
            ) {

                return false;
            }
        }

        return true;
    }

    // =========================================================
    // NORMALIZE
    // =========================================================

    public static String normalize(
            String replicationId
    ) {

        if (
                !isValid(
                        replicationId
                )
        ) {

            throw new IllegalArgumentException(
                    "Invalid replication ID"
            );
        }

        return replicationId
                .toLowerCase();
    }

    // =========================================================
    // MATCH
    // =========================================================

    public static boolean matches(
            String first,
            String second
    ) {

        if (
                first == null
                        ||
                second == null
        ) {

            return false;
        }

        return first.equalsIgnoreCase(
                second
        );
    }

    // =========================================================
    // SHORT DISPLAY
    // =========================================================

    public static String shortId(
            String replicationId
    ) {

        if (
                replicationId == null
                        ||
                replicationId.isBlank()
        ) {

            return "none";
        }

        if (
                replicationId.length()
                        <=
                8
        ) {

            return replicationId;
        }

        return replicationId.substring(
                0,
                8
        );
    }
}