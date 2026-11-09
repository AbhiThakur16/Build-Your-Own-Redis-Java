public enum NodeRole {

    MASTER,
    REPLICA;

    // =========================================================
    // CHECK MASTER
    // =========================================================

    public boolean isMaster() {

        return this == MASTER;
    }

    // =========================================================
    // CHECK REPLICA
    // =========================================================

    public boolean isReplica() {

        return this == REPLICA;
    }

    // =========================================================
    // PARSE ROLE
    // =========================================================

    public static NodeRole fromString(
            String value
    ) {

        if (
                value == null
                        ||
                value.isBlank()
        ) {

            throw new IllegalArgumentException(
                    "Node role cannot be empty"
            );
        }

        if (
                value.equalsIgnoreCase(
                        "master"
                )
        ) {

            return MASTER;
        }

        if (
                value.equalsIgnoreCase(
                        "replica"
                )
        ) {

            return REPLICA;
        }

        throw new IllegalArgumentException(
                "Unknown node role: "
                        + value
        );
    }

    // =========================================================
    // DISPLAY
    // =========================================================

    @Override
    public String toString() {

        return name();
    }
}