package io.eventuate.messaging.rabbitmq.consumer;

import io.eventuate.messaging.partitionmanagement.GroupMember;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkGroupMember implements GroupMember {
  private Logger logger = LoggerFactory.getLogger(getClass());

  private CuratorFramework curatorFramework;
  private String path;

  public ZkGroupMember(CuratorFramework curatorFramework, String groupId, String memberId) {
    this.curatorFramework = curatorFramework;
    this.path = ZkUtil.pathForGroupMember(groupId, memberId);

    try {
      logger.info("Initializing member: groupId = {}, memberId = {}", groupId, memberId);
      curatorFramework
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(path, new byte[0]);
      logger.info("Initialized member: groupId = {}, memberId = {}", groupId, memberId);
    } catch (Exception e) {
      logger.error("Initializing member failed: groupId = {}, memberId = {}", groupId, memberId);
      logger.error("Initializing member failed", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove() {
    try {
      logger.info("Removing member: path = {}", path);
      curatorFramework.delete().forPath(path);
      logger.info("Removed member: path = {}", path);
    } catch (Exception e) {
      logger.error("Removing member failed: path = {}", path);
      logger.error("Removing member failed", e);
      throw new RuntimeException(e);
    }
  }
}
