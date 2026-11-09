import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class KeyVersionManager {

    /*
     * key -> version
     *
     * Jab bhi key modify hoti hai,
     * uski version increase hoti hai.
     */
    private static final ConcurrentHashMap<String, Long> keyVersions =
            new ConcurrentHashMap<>();

    private static final AtomicLong versionCounter =
            new AtomicLong(0);


    private KeyVersionManager() {
    }


    // =========================================================
    // MARK KEY AS MODIFIED
    // =========================================================

    public static void touch(
            String key
    ) {

        if (
                key == null
                        ||
                key.isBlank()
        ) {

            return;
        }


        long newVersion =
                versionCounter.incrementAndGet();


        keyVersions.put(
                key,
                newVersion
        );
    }


    // =========================================================
    // GET CURRENT VERSION
    // =========================================================

    public static long getVersion(
            String key
    ) {

        return keyVersions.getOrDefault(
                key,
                0L
        );
    }


    // =========================================================
    // WATCH KEY
    // =========================================================

    public static void watch(
            Map<String, Long> watchedKeys,
            String key
    ) {

        watchedKeys.put(
                key,
                getVersion(key)
        );
    }


    // =========================================================
    // CHECK WATCHED KEYS
    // =========================================================

    public static boolean hasChanged(
            Map<String, Long> watchedKeys
    ) {

        for (
                Map.Entry<String, Long> entry :
                watchedKeys.entrySet()
        ) {

            long currentVersion =
                    getVersion(
                            entry.getKey()
                    );


            long watchedVersion =
                    entry.getValue();


            if (
                    currentVersion
                            !=
                    watchedVersion
            ) {

                return true;
            }
        }


        return false;
    }


    // =========================================================
    // CLEAR WATCHED KEYS
    // =========================================================

    public static void clear(
            Map<String, Long> watchedKeys
    ) {

        watchedKeys.clear();
    }


    // =========================================================
    // REMOVE VERSION
    // =========================================================

    public static void remove(
            String key
    ) {

        /*
         * Important:
         *
         * Delete bhi modification hai.
         * Isliye version completely delete nahi karenge.
         * Nayi version assign karenge.
         */
        touch(key);
    }


    // =========================================================
    // FLUSH ALL
    // =========================================================

    public static void flushAll(
            Collection<String> keys
    ) {

        if (keys == null) {
            return;
        }


        for (
                String key :
                keys
        ) {

            touch(key);
        }
    }


    // =========================================================
    // DEBUG
    // =========================================================

    public static int trackedKeyCount() {

        return keyVersions.size();
    }
}