package io.eventuate.messaging.rabbitmq.spring.consumer;

import java.nio.charset.StandardCharsets;

public class ZkUtil {
  public static String pathForAssignment(String groupId, String memberId) {
    return "/eventuate-tram/rabbitmq/consumer-assignments/%s/%s".formatted(groupId, memberId);
  }

  public static String pathForMemberGroup(String groupId) {
    return "/eventuate-tram/rabbitmq/consumer-groups/%s".formatted(groupId);
  }

  public static String pathForGroupMember(String groupId, String memberId) {
    return "/eventuate-tram/rabbitmq/consumer-groups/%s/%s".formatted(groupId, memberId);
  }

  public static String pathForLeader(String groupId) {
    return "/eventuate-tram/rabbitmq/consumer-leaders/%s".formatted(groupId);
  }

  public static String byteArrayToString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static byte[] stringToByteArray(String string) {
    return string.getBytes(StandardCharsets.UTF_8);
  }
}
