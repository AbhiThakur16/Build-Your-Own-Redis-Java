import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SortedSetStore implements Serializable {

    private static final long serialVersionUID = 1L;

    // member -> score
    private final ConcurrentHashMap<String, Double> members =
            new ConcurrentHashMap<>();


    // =========================================================
    // ZADD
    // =========================================================

    public int add(
            String member,
            double score
    ) {

        Double oldScore =
                members.put(
                        member,
                        score
                );

        // Redis ZADD:
        // 1 = new member
        // 0 = existing member updated
        return oldScore == null
                ? 1
                : 0;
    }


    // =========================================================
    // ZSCORE
    // =========================================================

    public Double getScore(
            String member
    ) {

        return members.get(
                member
        );
    }


    // =========================================================
    // ZREM
    // =========================================================

    public int remove(
            String member
    ) {

        return members.remove(
                member
        ) != null
                ? 1
                : 0;
    }


    // =========================================================
    // ZCARD
    // =========================================================

    public int size() {

        return members.size();
    }


    // =========================================================
    // IS EMPTY
    // =========================================================

    public boolean isEmpty() {

        return members.isEmpty();
    }


    // =========================================================
    // ZRANGE
    // =========================================================

    public List<String> range() {

        List<Map.Entry<String, Double>> entries =
                new ArrayList<>(
                        members.entrySet()
                );


        entries.sort(
                Comparator
                        .comparingDouble(
                                Map.Entry<String, Double>::getValue
                        )
                        .thenComparing(
                                Map.Entry::getKey
                        )
        );


        List<String> result =
                new ArrayList<>();


        for (
                Map.Entry<String, Double> entry :
                entries
        ) {

            result.add(
                    entry.getKey()
            );
        }


        return result;
    }


    // =========================================================
    // ZRANGE WITH SCORES
    // =========================================================

    public List<String> rangeWithScores() {

        List<Map.Entry<String, Double>> entries =
                new ArrayList<>(
                        members.entrySet()
                );


        entries.sort(
                Comparator
                        .comparingDouble(
                                Map.Entry<String, Double>::getValue
                        )
                        .thenComparing(
                                Map.Entry::getKey
                        )
        );


        List<String> result =
                new ArrayList<>();


        for (
                Map.Entry<String, Double> entry :
                entries
        ) {

            result.add(
                    entry.getKey()
            );

            result.add(
                    formatScore(
                            entry.getValue()
                    )
            );
        }


        return result;
    }


    // =========================================================
    // SNAPSHOT COPY
    // =========================================================

    public Map<String, Double> toMap() {

        return new HashMap<>(
                members
        );
    }


    // =========================================================
    // LOAD SNAPSHOT
    // =========================================================

    public void loadFromMap(
            Map<String, Double> data
    ) {

        members.clear();

        if (data != null) {

            members.putAll(
                    data
            );
        }
    }


    // =========================================================
    // SCORE FORMAT
    // =========================================================

    public static String formatScore(
            double score
    ) {

        if (
                score
                        ==
                Math.rint(score)
        ) {

            return String.valueOf(
                    (long) score
            );
        }

        return String.valueOf(
                score
        );
    }
}