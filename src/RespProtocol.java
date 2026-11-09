import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespProtocol {

    // =========================================================
    // READ COMMAND
    // Supports:
    // 1. Our old plain-text client
    // 2. Real Redis RESP protocol
    // =========================================================

    public static String readCommand(InputStream input)
            throws IOException {

        int firstByte = input.read();

        if (firstByte == -1) {
            return null;
        }

        // RESP Array starts with *
        if (firstByte == '*') {
            return readRespArray(input);
        }

        // Old plain-text command
        return readPlainCommand(input, firstByte);
    }


    // =========================================================
    // READ RESP ARRAY
    //
    // Example:
    //
    // *3
    // $3
    // SET
    // $4
    // name
    // $4
    // Abhi
    // =========================================================

    private static String readRespArray(InputStream input)
            throws IOException {

        String countLine = readLine(input);

        if (countLine == null) {
            return null;
        }

        int count;

        try {
            count = Integer.parseInt(countLine);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid RESP array length");
        }

        List<String> arguments =
                new ArrayList<>();

        for (int i = 0; i < count; i++) {

            int type = input.read();

            if (type == -1) {
                throw new EOFException();
            }

            // Bulk String
            if (type == '$') {

                String lengthLine =
                        readLine(input);

                int length;

                try {
                    length =
                            Integer.parseInt(lengthLine);
                } catch (NumberFormatException e) {
                    throw new IOException(
                            "Invalid bulk string length"
                    );
                }

                if (length == -1) {
                    arguments.add("");
                    continue;
                }

                byte[] data =
                        readExactly(input, length);

                // Consume \r\n
                int cr = input.read();
                int lf = input.read();

                if (cr != '\r' || lf != '\n') {
                    throw new IOException(
                            "Invalid RESP bulk string ending"
                    );
                }

                String value =
                        new String(
                                data,
                                StandardCharsets.UTF_8
                        );

                arguments.add(value);
            }

            // Simple String
            else if (type == '+') {

                arguments.add(
                        readLine(input)
                );
            }

            else {

                throw new IOException(
                        "Unsupported RESP type: "
                                + (char) type
                );
            }
        }

        return String.join(
                " ",
                arguments
        );
    }


    // =========================================================
    // OLD PLAIN-TEXT CLIENT SUPPORT
    // =========================================================

    private static String readPlainCommand(
            InputStream input,
            int firstByte
    ) throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        buffer.write(firstByte);

        while (true) {

            int b = input.read();

            if (b == -1) {
                break;
            }

            if (b == '\n') {
                break;
            }

            if (b != '\r') {
                buffer.write(b);
            }
        }

        return buffer.toString(
                StandardCharsets.UTF_8
        );
    }


    // =========================================================
    // READ LINE
    // =========================================================

    private static String readLine(InputStream input)
            throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        while (true) {

            int b = input.read();

            if (b == -1) {

                if (buffer.size() == 0) {
                    return null;
                }

                break;
            }

            if (b == '\n') {
                break;
            }

            if (b != '\r') {
                buffer.write(b);
            }
        }

        return buffer.toString(
                StandardCharsets.UTF_8
        );
    }


    // =========================================================
    // READ EXACT NUMBER OF BYTES
    // =========================================================

    private static byte[] readExactly(
            InputStream input,
            int length
    ) throws IOException {

        byte[] data =
                new byte[length];

        int offset = 0;

        while (offset < length) {

            int bytesRead =
                    input.read(
                            data,
                            offset,
                            length - offset
                    );

            if (bytesRead == -1) {
                throw new EOFException();
            }

            offset += bytesRead;
        }

        return data;
    }


    // =========================================================
    // RESP RESPONSE HELPERS
    // =========================================================

    public static void writeSimpleString(
            OutputStream output,
            String message
    ) throws IOException {

        write(
                output,
                "+"
                        + message
                        + "\r\n"
        );
    }


    public static void writeError(
            OutputStream output,
            String message
    ) throws IOException {

        write(
                output,
                "-ERR "
                        + message
                        + "\r\n"
        );
    }


    public static void writeInteger(
            OutputStream output,
            long number
    ) throws IOException {

        write(
                output,
                ":"
                        + number
                        + "\r\n"
        );
    }


    public static void writeBulkString(
            OutputStream output,
            String value
    ) throws IOException {

        if (value == null) {

            write(
                    output,
                    "$-1\r\n"
            );

            return;
        }

        byte[] bytes =
                value.getBytes(
                        StandardCharsets.UTF_8
                );

        write(
                output,
                "$"
                        + bytes.length
                        + "\r\n"
        );

        output.write(bytes);

        write(
                output,
                "\r\n"
        );

        output.flush();
    }


    public static void writeArray(
            OutputStream output,
            List<String> values
    ) throws IOException {

        write(
                output,
                "*"
                        + values.size()
                        + "\r\n"
        );

        for (String value : values) {

            writeBulkString(
                    output,
                    value
            );
        }
    }


    // =========================================================
    // WRITE
    // =========================================================

    private static void write(
            OutputStream output,
            String value
    ) throws IOException {

        output.write(
                value.getBytes(
                        StandardCharsets.UTF_8
                )
        );

        output.flush();
    }
}