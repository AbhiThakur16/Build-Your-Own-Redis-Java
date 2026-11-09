import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RedisSubscriber {

    private static final String HOST =
            "localhost";

    private static final int PORT =
            6379;


    private static final String PASSWORD =
            System.getenv()
                    .getOrDefault(
                            "JAVA_REDIS_PASSWORD",
                            "redis123"
                    );


    public static void main(
            String[] args
    ) {

        String channel;


        if (
                args.length > 0
        ) {

            channel =
                    args[0];

        } else {

            Scanner scanner =
                    new Scanner(
                            System.in
                    );


            System.out.print(
                    "Enter channel name: "
            );


            channel =
                    scanner.nextLine()
                            .trim();
        }


        if (
                channel.isEmpty()
        ) {

            System.out.println(
                    "Channel cannot be empty."
            );

            return;
        }


        try (
                Socket socket =
                        new Socket(
                                HOST,
                                PORT
                        )
        ) {

            InputStream input =
                    socket.getInputStream();

            OutputStream output =
                    socket.getOutputStream();


            System.out.println(
                    "Connected to Java Redis Server"
            );


            // =================================================
            // AUTO AUTH
            // =================================================

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


            // =================================================
            // SUBSCRIBE
            // =================================================

            sendArguments(
                    output,
                    Arrays.asList(
                            "SUBSCRIBE",
                            channel
                    )
            );


            System.out.println(
                    "Listening to channel: "
                            + channel
            );


            System.out.println(
                    "Press Ctrl+C to stop."
            );


            while (true) {

                Object response =
                        readResponse(
                                input
                        );


                if (
                        response == null
                ) {

                    System.out.println(
                            "Server disconnected."
                    );

                    break;
                }


                displayResponse(
                        response
                );
            }


        } catch (
                ConnectException e
        ) {

            System.out.println(
                    "Could not connect to Redis Server."
            );


            System.out.println(
                    "Make sure RedisServer is running on port 6379."
            );


        } catch (
                IOException e
        ) {

            System.out.println(
                    "Connection error: "
                            + e.getMessage()
            );
        }
    }


    // =========================================================
    // AUTH
    // =========================================================

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
                readResponse(
                        input
                );


        if (
                response
                        instanceof
                        String
        ) {

            return (
                    (String) response
            ).equalsIgnoreCase(
                    "OK"
            );
        }


        return false;
    }


    // =========================================================
    // SEND RESP
    // =========================================================

    private static void sendArguments(
            OutputStream output,
            List<String> arguments
    ) throws IOException {

        StringBuilder request =
                new StringBuilder();


        request.append("*")
                .append(
                        arguments.size()
                )
                .append(
                        "\r\n"
                );


        for (
                String argument :
                arguments
        ) {

            byte[] bytes =
                    argument.getBytes(
                            StandardCharsets.UTF_8
                    );


            request.append("$")
                    .append(
                            bytes.length
                    )
                    .append(
                            "\r\n"
                    );


            request.append(
                    argument
            );


            request.append(
                    "\r\n"
            );
        }


        output.write(
                request.toString()
                        .getBytes(
                                StandardCharsets.UTF_8
                        )
        );


        output.flush();
    }


    // =========================================================
    // READ RESP
    // =========================================================

    private static Object readResponse(
            InputStream input
    ) throws IOException {

        int type =
                input.read();


        if (
                type == -1
        ) {

            return null;
        }


        switch (type) {

            case '+':

                return readLine(
                        input
                );


            case '-':

                return "ERROR: "
                        + readLine(
                        input
                );


            case ':':

                return Long.parseLong(
                        readLine(
                                input
                        )
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

                return "Unknown RESP type: "
                        + (char) type;
        }
    }


    // =========================================================
    // ARRAY
    // =========================================================

    private static List<Object> readArray(
            InputStream input
    ) throws IOException {

        int count =
                Integer.parseInt(
                        readLine(
                                input
                        )
                );


        if (
                count == -1
        ) {

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
                    readResponse(
                            input
                    )
            );
        }


        return values;
    }


    // =========================================================
    // BULK STRING
    // =========================================================

    private static String readBulkString(
            InputStream input
    ) throws IOException {

        int length =
                Integer.parseInt(
                        readLine(
                                input
                        )
                );


        if (
                length == -1
        ) {

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


    // =========================================================
    // DISPLAY
    // =========================================================

    private static void displayResponse(
            Object response
    ) {

        if (
                !(
                        response
                                instanceof
                                List<?>
                )
        ) {

            System.out.println(
                    response
            );

            return;
        }


        List<?> values =
                (List<?>) response;


        if (
                values.isEmpty()
        ) {

            return;
        }


        String type =
                String.valueOf(
                        values.get(0)
                );


        // SUBSCRIBE
        if (
                type.equalsIgnoreCase(
                        "subscribe"
                )
        ) {

            if (
                    values.size() >= 3
            ) {

                System.out.println(
                        "Subscribed to: "
                                + values.get(1)
                );


                System.out.println(
                        "Subscriptions: "
                                + values.get(2)
                );
            }


            return;
        }


        // UNSUBSCRIBE
        if (
                type.equalsIgnoreCase(
                        "unsubscribe"
                )
        ) {

            if (
                    values.size() >= 2
            ) {

                System.out.println(
                        "Unsubscribed from: "
                                + values.get(1)
                );
            }


            return;
        }


        // MESSAGE
        if (
                type.equalsIgnoreCase(
                        "message"
                )
                        &&
                values.size() >= 3
        ) {

            System.out.println();


            System.out.println(
                    "Message ["
                            + values.get(1)
                            + "]: "
                            + values.get(2)
            );


            return;
        }


        System.out.println(
                values
        );
    }


    // =========================================================
    // READ LINE
    // =========================================================

    private static String readLine(
            InputStream input
    ) throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();


        while (true) {

            int b =
                    input.read();


            if (
                    b == -1
            ) {

                break;
            }


            if (
                    b == '\n'
            ) {

                break;
            }


            if (
                    b != '\r'
            ) {

                buffer.write(
                        b
                );
            }
        }


        return buffer.toString(
                StandardCharsets.UTF_8
        );
    }


    // =========================================================
    // READ EXACT
    // =========================================================

    private static byte[] readExactly(
            InputStream input,
            int length
    ) throws IOException {

        byte[] data =
                new byte[length];


        int offset =
                0;


        while (
                offset < length
        ) {

            int read =
                    input.read(
                            data,
                            offset,
                            length - offset
                    );


            if (
                    read == -1
            ) {

                throw new EOFException(
                        "Unexpected end of stream"
                );
            }


            offset +=
                    read;
        }


        return data;
    }
}