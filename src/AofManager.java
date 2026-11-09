import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class AofManager {

    private static final Object LOCK = new Object();

    private static volatile String fileName =
            "appendonly.aof";

    private AofManager() {
    }

    public static void configure(String name) {

        if (name != null && !name.isBlank()) {
            fileName = name;
        }
    }

    public static void appendCommand(String command) {

        if (command == null || command.isBlank()) {
            return;
        }

        synchronized (LOCK) {

            String[] arguments =
                    command.trim().split("\\s+");

            try (
                    FileOutputStream fileOutput =
                            new FileOutputStream(
                                    fileName,
                                    true
                            )
            ) {

                writeArguments(
                        fileOutput,
                        Arrays.asList(arguments)
                );

                fileOutput.flush();
                fileOutput.getFD().sync();

            } catch (IOException e) {

                System.out.println(
                        "AOF write error: "
                                + e.getMessage()
                );
            }
        }
    }

    private static void writeArguments(
            OutputStream output,
            List<String> arguments
    ) throws IOException {

        output.write(
                ("*" + arguments.size() + "\r\n")
                        .getBytes(StandardCharsets.UTF_8)
        );

        for (String argument : arguments) {

            byte[] bytes =
                    argument.getBytes(
                            StandardCharsets.UTF_8
                    );

            output.write(
                    ("$" + bytes.length + "\r\n")
                            .getBytes(StandardCharsets.UTF_8)
            );

            output.write(bytes);

            output.write(
                    "\r\n".getBytes(
                            StandardCharsets.UTF_8
                    )
            );
        }
    }

    public static List<String> readCommands() {

        List<String> commands =
                new ArrayList<>();

        File file =
                new File(fileName);

        if (!file.exists()) {
            return commands;
        }

        synchronized (LOCK) {

            try (
                    InputStream input =
                            new FileInputStream(file)
            ) {

                while (true) {

                    String command =
                            RespProtocol.readCommand(input);

                    if (command == null) {
                        break;
                    }

                    command = command.trim();

                    if (!command.isEmpty()) {
                        commands.add(command);
                    }
                }

            } catch (IOException e) {

                System.out.println(
                        "AOF read error: "
                                + e.getMessage()
                );
            }
        }

        return commands;
    }

    public static void clear() {

        synchronized (LOCK) {

            try (
                    FileOutputStream ignored =
                            new FileOutputStream(
                                    fileName,
                                    false
                            )
            ) {

                // File truncate.

            } catch (IOException e) {

                System.out.println(
                        "AOF clear error: "
                                + e.getMessage()
                );
            }
        }
    }

    public static long getFileSize() {

        return new File(fileName).length();
    }

    public static long getCommandCount() {

        return readCommands().size();
    }

    public static String getFileName() {

        return fileName;
    }
}