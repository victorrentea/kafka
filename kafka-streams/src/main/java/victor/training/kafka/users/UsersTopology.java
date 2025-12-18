package victor.training.kafka.users;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.common.serialization.Serdes.String;

/**
 * Topology that reads chat lines from {@link #CHAT_LINES_TOPIC}, detects the leading username
 * (prefixed with '@' or '&'), looks up the full name in the {@link #USERS_TOPIC} KTable
 * (key = username, value = {@link User}), and emits the sentence with the username replaced
 * to {@link #CHAT_LINES_RESOLVED_TOPIC}. Messages without a mapping are filtered out.
 */
@Slf4j
public class UsersTopology {
  public static final String CHAT_LINES_TOPIC = "chat-lines";
  public static final String USERS_TOPIC = "users";
  public static final String CHAT_LINES_RESOLVED_TOPIC = "chat-lines-resolved";

  /** Matches a mention at the beginning of the line: '@user' or '&user', capturing username and suffix. */
  private static final Pattern MENTION = Pattern.compile("^[@&]([^\\s]+)(.*)$");

  public record User(String username, String fullName, String email) {}

  public static void createTopology(StreamsBuilder builder) {
    // Users KTable: (username -> User)
    KTable<String, User> users = builder.table(USERS_TOPIC, Consumed.with(String(), new JsonSerde<>(User.class)));

    // Extract the username as key and keep the rest of the sentence (suffix) as value
    builder.stream(CHAT_LINES_TOPIC, Consumed.with(String(), String()))
        .map(UsersTopology::extractUsernameAndSuffix)
        .filter((username, suffix1) -> username != null)
        .join(users,
            (suffix, user) -> user == null ? null : user.fullName + suffix,
            Joined.with(String(), String(), new JsonSerde<>(User.class)))
        .filter((k, resolved) -> resolved != null)
        // set the key to null to avoid propagating the username as the output key
        .map((k, resolved) -> KeyValue.<String, String>pair(null, resolved))
        .to(CHAT_LINES_RESOLVED_TOPIC, Produced.with(String(), String()));
  }

  /**
   * Extracts the mention username (without the leading '@' or '&') as the new key and keeps the
   * remainder of the sentence (including whitespace) as the value. If no mention is found, returns
   * a pair with null key and the original value.
   */
  private static KeyValue<String, String> extractUsernameAndSuffix(Object ignoredKey, String value) {
    if (value == null) return KeyValue.pair(null, null);
    Matcher m = MENTION.matcher(value);
    if (m.matches()) {
      String username = m.group(1); // without @/&
      String suffix = m.group(2);   // the rest of the sentence including subsequent spaces
      return KeyValue.pair(username, suffix);
    }
    return KeyValue.pair(null, value);
  }
}
