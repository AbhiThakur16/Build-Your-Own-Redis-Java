import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PubSubManager {

    // channel -> subscribers
    private static final ConcurrentHashMap<
            String,
            Set<Subscriber>
            > channels = new ConcurrentHashMap<>();


    // =========================================================
    // SUBSCRIBER
    // =========================================================

    public static class Subscriber {

        private final OutputStream output;

        private final Set<String> subscribedChannels =
                ConcurrentHashMap.newKeySet();

        private final Object writeLock =
                new Object();

        private Subscriber(OutputStream output) {

            this.output = output;
        }

        public int getSubscriptionCount() {

            return subscribedChannels.size();
        }
    }


    // =========================================================
    // CREATE SUBSCRIBER
    // =========================================================

    public static Subscriber createSubscriber(
            OutputStream output
    ) {

        return new Subscriber(output);
    }


    // =========================================================
    // SUBSCRIBE
    // =========================================================

    public static void subscribe(
            Subscriber subscriber,
            String channel
    ) throws IOException {

        Set<Subscriber> channelSubscribers =
                channels.computeIfAbsent(
                        channel,
                        k -> ConcurrentHashMap.newKeySet()
                );

        channelSubscribers.add(subscriber);

        subscriber.subscribedChannels.add(
                channel
        );

        sendArray(
                subscriber,
                Arrays.asList(
                        "subscribe",
                        channel,
                        String.valueOf(
                                subscriber.getSubscriptionCount()
                        )
                )
        );
    }


    // =========================================================
    // UNSUBSCRIBE
    // =========================================================

    public static void unsubscribe(
            Subscriber subscriber,
            String channel
    ) throws IOException {

        Set<Subscriber> channelSubscribers =
                channels.get(channel);

        if (channelSubscribers != null) {

            channelSubscribers.remove(
                    subscriber
            );

            if (channelSubscribers.isEmpty()) {

                channels.remove(channel);
            }
        }

        subscriber.subscribedChannels.remove(
                channel
        );

        sendArray(
                subscriber,
                Arrays.asList(
                        "unsubscribe",
                        channel,
                        String.valueOf(
                                subscriber.getSubscriptionCount()
                        )
                )
        );
    }


    // =========================================================
    // UNSUBSCRIBE ALL
    // =========================================================

    public static void unsubscribeAll(
            Subscriber subscriber
    ) throws IOException {

        List<String> subscribed =
                new ArrayList<>(
                        subscriber.subscribedChannels
                );

        if (subscribed.isEmpty()) {

            sendArray(
                    subscriber,
                    Arrays.asList(
                            "unsubscribe",
                            "",
                            "0"
                    )
            );

            return;
        }

        for (String channel : subscribed) {

            unsubscribe(
                    subscriber,
                    channel
            );
        }
    }


    // =========================================================
    // PUBLISH
    // =========================================================

    public static int publish(
            String channel,
            String message
    ) {

        Set<Subscriber> subscribers =
                channels.get(channel);

        if (
                subscribers == null
                        ||
                subscribers.isEmpty()
        ) {

            return 0;
        }

        int delivered = 0;

        for (
                Subscriber subscriber :
                subscribers
        ) {

            try {

                sendArray(
                        subscriber,
                        Arrays.asList(
                                "message",
                                channel,
                                message
                        )
                );

                delivered++;

            } catch (IOException e) {

                removeSubscriber(
                        subscriber
                );
            }
        }

        return delivered;
    }


    // =========================================================
    // REMOVE DISCONNECTED SUBSCRIBER
    // =========================================================

    public static void removeSubscriber(
            Subscriber subscriber
    ) {

        for (
                String channel :
                new ArrayList<>(
                        subscriber.subscribedChannels
                )
        ) {

            Set<Subscriber> subscribers =
                    channels.get(channel);

            if (subscribers != null) {

                subscribers.remove(
                        subscriber
                );

                if (subscribers.isEmpty()) {

                    channels.remove(
                            channel
                    );
                }
            }
        }

        subscriber.subscribedChannels.clear();
    }


    // =========================================================
    // CHANNEL COUNT
    // =========================================================

    public static int getChannelCount() {

        return channels.size();
    }


    // =========================================================
    // SAFE WRITE
    // =========================================================

    private static void sendArray(
            Subscriber subscriber,
            List<String> values
    ) throws IOException {

        synchronized (
                subscriber.writeLock
        ) {

            RespProtocol.writeArray(
                    subscriber.output,
                    values
            );
        }
    }
}