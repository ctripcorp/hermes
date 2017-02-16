DROP TABLE IF EXISTS `meta`;

CREATE TABLE `meta` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `value` text NOT NULL,
  `DataChange_LastTime` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table schema
# ------------------------------------------------------------

DROP TABLE IF EXISTS `schema`;

CREATE TABLE `schema` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(500) NOT NULL COMMENT 'SchemaRegistry里的subject，应为{topic}-value命名',
  `type` varchar(500) NOT NULL COMMENT 'JSON/AVRO',
  `topicId` bigint(20) NOT NULL,
  `version` int(11) unsigned NOT NULL COMMENT '如果是Avro，应该与SchemaRegistry里一致',
  `description` varchar(5000) DEFAULT NULL,
  `compatibility` varchar(50) DEFAULT NULL COMMENT 'NONE, FULL, FORWARD, BACKWARD',
  `create_time` datetime NOT NULL,
  `schema_content` mediumblob COMMENT 'JSON/AVRO 描述文件',
  `schema_properties` text,
  `jar_content` mediumblob COMMENT 'JAR 下载使用',
  `jar_properties` text,
  `avroid` int(11) unsigned DEFAULT NULL COMMENT '关联到Schema-Registry',
  `DataChange_LastTime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table subscription
# ------------------------------------------------------------

DROP TABLE IF EXISTS `subscription`;

CREATE TABLE `subscription` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(500) NOT NULL,
  `topic` varchar(500) NOT NULL,
  `group` varchar(500) NOT NULL,
  `endpoints` varchar(500) NOT NULL,
  `DataChange_LastTime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

