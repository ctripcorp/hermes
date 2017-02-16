/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;

CREATE DATABASE IF NOT EXISTS `fxhermesmetadb` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `fxhermesmetadb`;


CREATE TABLE IF NOT EXISTS `meta` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT 'pk:id',
  `value` text NOT NULL COMMENT 'value',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modified time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hello';

CREATE TABLE IF NOT EXISTS `schema` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk:id',
  `name` varchar(500) NOT NULL DEFAULT '-1' COMMENT 'SchemaRegistry里的subject，应为{topic}-value命名',
  `type` varchar(500) NOT NULL DEFAULT '-1' COMMENT 'JSON/AVRO',
  `topicId` bigint(20) NOT NULL DEFAULT '-1' COMMENT 'topicId',
  `version` int(11) unsigned NOT NULL DEFAULT '1' COMMENT '如果是Avro，应该与SchemaRegistry里一致',
  `description` varchar(5000) DEFAULT NULL COMMENT 'description on this schema',
  `compatibility` varchar(50) DEFAULT NULL COMMENT 'NONE, FULL, FORWARD, BACKWARD',
  `create_time` datetime DEFAULT NULL COMMENT 'this schema create time',
  `schema_content` mediumblob COMMENT 'JSON/AVRO 描述文件',
  `schema_properties` text COMMENT 'schema properties',
  `jar_content` mediumblob COMMENT 'JAR 下载使用',
  `jar_properties` text COMMENT 'jar properties',
  `cs_content` MEDIUMBLOB NULL COMMENT 'CS下载使用',
  `cs_properties` text NULL COMMENT 'cs properties',
  `avroid` int(11) unsigned DEFAULT NULL COMMENT '关联到Schema-Registry',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='for schema';

CREATE TABLE IF NOT EXISTS `subscription` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'name',
  `topic` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'topic',
  `group` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'group',
  `endpoints` varchar(500) CHARACTER SET latin1 NOT NULL DEFAULT 'null' COMMENT 'endpoint',
  `status` varchar(500) NOT NULL DEFAULT 'null' COMMENT 'status',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='subscription';

CREATE TABLE IF NOT EXISTS `monitor_event` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `event_type` int(11) NOT NULL,
  `create_time` datetime DEFAULT NULL,
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  `key1` varchar(4096) DEFAULT NULL,
  `key2` varchar(4096) DEFAULT NULL,
  `key3` varchar(4096) DEFAULT NULL,
  `key4` varchar(4096) DEFAULT NULL,
  `message` longtext,
  `notify_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='monitor_event';

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;

/* Meta Refactor */

ALTER TABLE `meta`
         ADD COLUMN `version` BIGINT NULL DEFAULT NULL COMMENT '版本号' AFTER `DataChange_LastTime`;

CREATE TABLE `app` (
         `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='接入应用，备用'
ENGINE=InnoDB
;
CREATE TABLE `codec` (
         `type` VARCHAR(50) NOT NULL DEFAULT 'json' COMMENT 'json/avro',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`type`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='codec'
ENGINE=InnoDB
;
CREATE TABLE `datasource` (
         `id` VARCHAR(200) NOT NULL DEFAULT 'ds0' COMMENT 'pk',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `storage_type` VARCHAR(50) NULL DEFAULT NULL COMMENT '存储类型',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='datasource'
ENGINE=InnoDB
;
CREATE TABLE `endpoint` (
         `id` VARCHAR(50) NOT NULL DEFAULT 'br0' COMMENT 'pk',
         `type` VARCHAR(500) NOT NULL DEFAULT 'broker' COMMENT 'type',
         `host` VARCHAR(500) NULL DEFAULT NULL COMMENT 'host',
         `port` SMALLINT(6) NULL DEFAULT NULL COMMENT 'port',
         `group` VARCHAR(50) NULL DEFAULT NULL COMMENT 'group',
         `idc` varchar(50) NULL DEFAULT NULL COMMENT 'idc',
		 `enabled` bit(1) NULL DEFAULT NULL COMMENT '是否启用',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='endpoint'
ENGINE=InnoDB
;

CREATE TABLE `producer` (
         `app_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`app_id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='producer'
ENGINE=InnoDB
;
CREATE TABLE `server` (
         `id` VARCHAR(50) NOT NULL DEFAULT 'host1' COMMENT 'pk',
         `host` VARCHAR(500) NOT NULL DEFAULT 'localhost' COMMENT 'host',
         `port` SMALLINT(6) NOT NULL DEFAULT '1248' COMMENT 'port',
         `enabled` bit(1) NULL DEFAULT NULL COMMENT 'enabled',
         `idc` varchar(50) NULL DEFAULT 'idc1' COMMENT 'idc',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='server'
ENGINE=InnoDB
;
CREATE TABLE `storage` (
         `type` VARCHAR(50) NOT NULL DEFAULT 'mysql' COMMENT 'pk',
         `default` BIT(1) NOT NULL DEFAULT b'0' COMMENT '是否缺省',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT '属性',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`type`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='storage'
ENGINE=InnoDB
;
CREATE TABLE `topic` (
         `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `name` VARCHAR(500) NOT NULL DEFAULT 'name' COMMENT 'name',
         `partition_count` SMALLINT(6) NULL DEFAULT '0' COMMENT 'partition_count',
         `storage_type` VARCHAR(50) NULL DEFAULT NULL COMMENT 'storage_type',
         `description` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'description',
         `status` VARCHAR(50) NULL DEFAULT NULL COMMENT 'status',
         `create_time` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'create_time',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `schema_id` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'schema_id',
         `consumer_retry_policy` VARCHAR(500) NULL DEFAULT NULL COMMENT 'consumer_retry_policy',
         `owner_1` VARCHAR(500) NULL DEFAULT NULL COMMENT 'owner_1',
         `owner_2` VARCHAR(500) NULL DEFAULT NULL COMMENT 'owner_2',
         `phone_1` VARCHAR(20) NULL DEFAULT NULL COMMENT 'phone_1',
         `phone_2` VARCHAR(20) NULL DEFAULT NULL COMMENT 'phone_2',
         `endpoint_type` VARCHAR(500) NULL DEFAULT NULL COMMENT 'endpoint_type',
         `ack_timeout_seconds` INT(11) NULL DEFAULT NULL COMMENT 'ack_timeout_seconds',
         `codec_type` VARCHAR(50) NULL DEFAULT NULL COMMENT 'codec_type',
         `other_info` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'other_info',
         `storage_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'storage_partition_size',
         `resend_partition_size` BIGINT(20) UNSIGNED NULL DEFAULT NULL COMMENT 'resend_partition_size',
         `storage_partition_count` INT(10) UNSIGNED NULL DEFAULT NULL COMMENT 'storage_partition_count',
         `properties` VARCHAR(5000) NULL DEFAULT NULL COMMENT 'properties',
         `priority_message_enabled` BIT(1) NULL DEFAULT NULL COMMENT 'priority_message_enabled',
         `broker_group` VARCHAR(50) NULL DEFAULT NULL COMMENT 'broker分组信息',
         `idc_policy` VARCHAR(50) NULL DEFAULT NULL COMMENT 'idc policy',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='topic'
ENGINE=InnoDB
;
CREATE TABLE `partition` (
         `partition_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         `id` INT(10) UNSIGNED NOT NULL DEFAULT '1' COMMENT 'start from 1',
         `read_datasource` VARCHAR(500) NOT NULL DEFAULT 'ds0' COMMENT 'read ds',
         `write_datasource` VARCHAR(500) NOT NULL DEFAULT 'ds0' COMMENT 'write ds',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         PRIMARY KEY (`partition_id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`),
         INDEX `topic_id` (`topic_id`),
         INDEX `id` (`id`)
)
COMMENT='partition'
ENGINE=InnoDB
;
CREATE TABLE `consumer_group` (
         `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
         `name` VARCHAR(500) NOT NULL DEFAULT 'name' COMMENT 'name',
         `app_Ids` VARCHAR(500) NULL DEFAULT NULL COMMENT '应用id',
         `retry_policy` VARCHAR(500) NULL DEFAULT NULL COMMENT '重试',
         `ack_timeout_seconds` INT(11) NULL DEFAULT NULL COMMENT 'ack超时',
         `ordered_consume` BIT(1) NULL DEFAULT NULL COMMENT '是否有序',
         `owner_1` VARCHAR(500) NULL DEFAULT NULL COMMENT 'owner_1',
         `owner_2` VARCHAR(500) NULL DEFAULT NULL COMMENT 'owner_2',
         `phone_1` VARCHAR(20) NULL DEFAULT NULL COMMENT 'phone_1',
         `phone_2` VARCHAR(20) NULL DEFAULT NULL COMMENT 'phone_2',
         `DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
         `topic_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'topic id',
         `idc_policy` VARCHAR(50) NULL DEFAULT NULL COMMENT 'idc policy',
         PRIMARY KEY (`id`),
         INDEX `DataChange_LastTime` (`DataChange_LastTime`),
         INDEX `topic_id` (`topic_id`)
)
COMMENT='consumer_group'
ENGINE=InnoDB
;
CREATE TABLE `application` (
		`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'id',
		`type` INT(11) NOT NULL DEFAULT '-1' COMMENT '创建Topic:0, 创建Consumer:1, 修改Topic:2, 修改Consumer:3',
		`status` INT(11) NOT NULL DEFAULT '-1' COMMENT '待审批:0, 拒绝:1, 成功:2',
		`content` VARCHAR(1024) NULL DEFAULT NULL COMMENT 'Topic/Consumer详细参数',
		`comment` VARCHAR(1024) NULL DEFAULT NULL COMMENT '修改意见',
		`owner_1` VARCHAR(128) NULL DEFAULT NULL COMMENT '申请人邮箱1',
		`owner_2` VARCHAR(128) NULL DEFAULT NULL COMMENT '申请人邮箱2',
		`approver` VARCHAR(128) NULL DEFAULT NULL COMMENT '审批人',
		`create_time` DATETIME NULL DEFAULT NULL COMMENT 'this application create time',
		`DataChange_LastTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
		PRIMARY KEY (`id`),
		INDEX `DataChange_LastTime` (`DataChange_LastTime`)
)
COMMENT='for application'
ENGINE=InnoDB
;

CREATE TABLE `kv` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `k` varchar(255) NOT NULL DEFAULT '' COMMENT 'key',
  `v` text COMMENT 'value',
  `tag` varchar(64) DEFAULT NULL COMMENT 'kv tag',
  `creation_date` datetime NOT NULL COMMENT 'create time',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  PRIMARY KEY (`id`),
  KEY `KEY` (`k`)
)
COMMENT='hermes kv storage'
ENGINE=InnoDB 
;

CREATE TABLE `notification` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `ref_key` varchar(100) NOT NULL DEFAULT '' COMMENT 'reference key of notification',
  `notification_type` varchar(50) NOT NULL COMMENT 'notification type: email, sms ...',
  `create_time` datetime DEFAULT NULL COMMENT 'create time',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  `content` mediumtext COMMENT 'notification content',
  `notify_time` datetime DEFAULT NULL COMMENT 'notify time',
  `receivers` text COMMENT 'notification receivers',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`),
  KEY `ref_key` (`ref_key`)
) 
COMMENT='hermes notification storage'
ENGINE=InnoDB
;

CREATE TABLE `consumer_monitor_config` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `topic` varchar(500) NOT NULL DEFAULT '' COMMENT 'topic名称',
  `consumer` varchar(500) NOT NULL DEFAULT '' COMMENT 'consumer名称',
  `large_backlog_enable` tinyint(1) DEFAULT '1' COMMENT '是否开启积压',
  `large_backlog_limit` int(11) DEFAULT '-1' COMMENT '积压阈值',
  `large_deadletter_enable` tinyint(1) DEFAULT '1' COMMENT '是否开启死信',
  `large_deadletter_limit` int(11) DEFAULT '-1' COMMENT '死信阈值',
  `large_delay_enable` tinyint(1) DEFAULT '1' COMMENT '是否开启延迟',
  `large_delay_limit` int(11) DEFAULT '-1' COMMENT '延迟阈值',
  `long_time_no_consume_enable` tinyint(1) DEFAULT '1' COMMENT '是否开启太久不消费',
  `long_time_no_consume_limit` int(11) DEFAULT '-1' COMMENT '太久不消费阈值',
  `alarm_receivers` varchar(4096) DEFAULT NULL COMMENT '报警接收人',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '上次修改时间',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='consumer monitor configs';

CREATE TABLE `producer_monitor_config` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'item id',
  `topic` varchar(500) NOT NULL DEFAULT '' COMMENT 'topic name',
  `long_time_no_produce_enable` tinyint(1) DEFAULT '1' COMMENT 'enable long time no produce',
  `long_time_no_produce_limit` int(11) DEFAULT '-1' COMMENT 'long time no produce limit',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last modify time',
  `create_time` datetime DEFAULT NULL COMMENT 'create time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='producer monitor configs';

ALTER TABLE `consumer_group`
	ADD COLUMN `enabled` BIT(1)  COMMENT '是否启用';

CREATE TABLE `idc` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'idc id',
  `name` varchar(50) NOT NULL DEFAULT 'name' COMMENT 'idc name',
  `display_name` varchar(50) NULL DEFAULT NULL COMMENT 'idc display name',
  `primary` bit(1) NOT NULL DEFAULT b'0' COMMENT '是否Primary',
  `enabled` bit(1) NOT NULL DEFAULT b'1' COMMENT '是否enabled',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'last change time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='idc信息'

CREATE TABLE `zookeeper_ensemble` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `name` varchar(128) NOT NULL DEFAULT '' COMMENT 'zk集群名称',
  `connection_string` varchar(2048) NOT NULL DEFAULT '' COMMENT 'zk连接字符串',
  `idc` varchar(50) NOT NULL DEFAULT '' COMMENT '机房',
  `primary` bit(1) NOT NULL DEFAULT b'0' COMMENT 'is primary',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
  PRIMARY KEY (`id`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COMMENT='zookeeper信息';

CREATE TABLE `broker_lease` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `topic` varchar(250) NOT NULL DEFAULT '' COMMENT 'topic name',
  `partition` int(11) NOT NULL COMMENT 'partition',
  `leases` varchar(20000) DEFAULT '{}' COMMENT 'leases',
  `metaserver` varchar(15) NOT NULL DEFAULT '' COMMENT 'metaserver ip',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `topic_partition` (`topic`,`partition`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='broker lease信息';

CREATE TABLE `consumer_lease` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
  `topic` varchar(250) NOT NULL DEFAULT '' COMMENT 'topic name',
  `partition` int(11) NOT NULL COMMENT 'partition',
  `group` varchar(250) DEFAULT NULL COMMENT 'consumer group',
  `leases` varchar(20000) DEFAULT '{}' COMMENT 'leases',
  `metaserver` varchar(15) NOT NULL DEFAULT '' COMMENT 'metaserver ip',
  `DataChange_LastTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'changed time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `topic_partition_group` (`topic`,`partition`,`group`),
  KEY `DataChange_LastTime` (`DataChange_LastTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='consumer lease信息';