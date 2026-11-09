import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RedisClient {

    private static final String HOST =
            "localhost";

    private static final String PASSWORD =
            System.getenv()
                    .getOrDefault(
                            "JAVA_REDIS_PASSWORD",
                            "redis123"
                    );

    public static void main(String[] args) {

        int port =
                6379;

        if (args.length > 0) {

            try {

                port =
                        Integer.parseInt(
                                args[0]
                        );

            } catch (NumberFormatException e) {

                System.out.println(
                        "Invalid port."
                );

                return;
            }
        }

        try (
                Socket socket =
                        new Socket(
                                HOST,
                                port
                        )
        ) {

            InputStream input =
                    socket.getInputStream();

            OutputStream output =
                    socket.getOutputStream();

            System.out.println(
                    "Connected to Java Redis Server on port "
                            + port
            );

            if (
                    !authenticate(
                            input,
                            output
                    )
            ) {

                System.out.println(
                        "Authentication failed."
                );

                return;
            }

            System.out.println(
                    "Authentication successful."
            );

            System.out.println(
                    "Type commands. Type EXIT to close."
            );

            Scanner scanner =
                    new Scanner(System.in);

            while (true) {

                System.out.print(
                        "redis> "
                );

                if (!scanner.hasNextLine()) {
                    break;
                }

                String command =
                        scanner.nextLine()
                                .trim();

                if (command.isEmpty()) {
                    continue;
                }

                if (
                        command.equalsIgnoreCase(
                                "EXIT"
                        )
                ) {

                    System.out.println(
                            "Closing Redis Client..."
                    );

                    break;
                }

                if (
                        command.toUpperCase()
                                .startsWith(
                                        "AUTH "
                                )
                ) {

                    System.out.println(
                            "Authentication is handled automatically."
                    );

                    continue;
                }

                sendCommand(
                        output,
                        command
                );

                Object response =
                        readResponse(input);

                displayResponse(
                        response
                );
            }

        } catch (ConnectException e) {

            System.out.println(
                    "Could not connect to Redis Server."
            );

        } catch (IOException e) {

            System.out.println(
                    "Connection error: "
                            + e.getMessage()
            );
        }
    }

    private static boolean authenticate(
            InputStream input,
            OutputStream output
    ) throws IOException {

        sendArguments(
                output,
                Arrays.asList(
                        "AUTH",
                        PASSWORD
                )
        );

        Object response =
                readResponse(input);

        return response instanceof String
                &&
                response
                        .toString()
                        .equalsIgnoreCase(
                                "OK"
                        );
    }

    private static void sendCommand(
            OutputStream output,
            String command
    ) throws IOException {

        String[] parts =
                command.split("\\s+");

        sendArguments(
                output,
                Arrays.asList(parts)
        );
    }

    private static void sendArguments(
            OutputStream output,
            List<String> arguments
    ) throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        buffer.write(
                (
                        "*"
                                + arguments.size()
                                + "\r\n"
                ).getBytes(StandardCharsets.UTF_8)
        );

        for (String argument : arguments) {

            byte[] bytes =
                    argument.getBytes(
                            StandardCharsets.UTF_8
                    );

            buffer.write(
                    (
                            "$"
                                    + bytes.length
                                    + "\r\n"
                    ).getBytes(StandardCharsets.UTF_8)
            );

            buffer.write(bytes);

            buffer.write(
                    "\r\n".getBytes(
                            StandardCharsets.UTF_8
                    )
            );
        }

        output.write(
                buffer.toByteArray()
        );

        output.flush();
    }

    private static Object readResponse(
            InputStream input
    ) throws IOException {

        int type =
                input.read();

        if (type == -1) {
            return null;
        }

        switch (type) {

            case '+':

                return readLine(input);

            case '-':

                return "ERROR: "
                        + readLine(input);

            case ':':

                return Long.parseLong(
                        readLine(input)
                );

            case '$':

                return readBulkString(
                        input
                );

            case '*':

                return readArray(
                        input
                );

            default:

                return "Unknown RESP response: "
                        + (char) type;
        }
    }

    private static List<Object> readArray(
            InputStream input
    ) throws IOException {

        int count =
                Integer.parseInt(
                        readLine(input)
                );

        if (count == -1) {
            return null;
        }

        List<Object> values =
                new ArrayList<>();

        for (
                int i = 0;
                i < count;
                i++
        ) {

            values.add(
                    readResponse(input)
            );
        }

        return values;
    }

    private static String readBulkString(
            InputStream input
    ) throws IOException {

        int length =
                Integer.parseInt(
                        readLine(input)
                );

        if (length == -1) {
            return null;
        }

        byte[] bytes =
                readExactly(
                        input,
                        length
                );

        input.read();
        input.read();

        return new String(
                bytes,
                StandardCharsets.UTF_8
        );
    }

    private static void displayResponse(
            Object response
    ) {

        if (response == null) {

            System.out.println(
                    "(nil)"
            );

            return;
        }

        if (
                response
                        instanceof
                        List<?>
        ) {

            List<?> list =
                    (List<?>) response;

            if (list.isEmpty()) {

                System.out.println(
                        "(empty)"
                );

                return;
            }

            int index =
                    1;

            for (Object value : list) {

                System.out.println(
                        index
                                + ") "
                                +
                        (
                                value == null
                                        ? "(nil)"
                                        : value
                        )
                );

                index++;
            }

            return;
        }

        System.out.println(
                response
        );
    }

    private static String readLine(
            InputStream input
    ) throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        while (true) {

            int b =
                    input.read();

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

    private static byte[] readExactly(
            InputStream input,
            int length
    ) throws IOException {

        byte[] data =
                new byte[length];

        int offset =
                0;

        while (offset < length) {

            int count =
                    input.read(
                            data,
                            offset,
                            length - offset
                    );

            if (count == -1) {

                throw new EOFException(
                        "Unexpected end of stream"
                );
            }

            offset += count;
        }

        return data;
    }
}