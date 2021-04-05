/*
SQLyog Ultimate v12.2.6 (64 bit)
MySQL - 5.7.25-0ubuntu0.18.10.2 : Database - kts
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`kts` /*!40100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin */;

USE `kts`;

/*Table structure for table `files_location` */

DROP TABLE IF EXISTS `files_location`;

CREATE TABLE `files_location` (
  `tape_no` bigint(20) unsigned NOT NULL COMMENT 'номер кассеты',
  `file_uid` bigint(20) unsigned NOT NULL COMMENT 'uid файла',
  `tape_record_no` bigint(20) unsigned NOT NULL COMMENT 'номер записи на кассете',
  `saved` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'дата записи файла на кассете',
  `record_type` tinyint(3) unsigned NOT NULL COMMENT 'тип записи - файлом или в составе архива',
  `deleted` tinyint(1) unsigned NOT NULL COMMENT 'удален',
  `todelete` tinyint(1) unsigned NOT NULL COMMENT 'необходимо удалить с кассеты',
  `size` bigint(20) unsigned NOT NULL COMMENT 'размер записанного файла',
  `mtime` timestamp NULL DEFAULT NULL COMMENT 'modification time',
  `broken` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT 'ошибка при чтении'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Data for the table `files_location` */

/*Table structure for table `root_index` */

DROP TABLE IF EXISTS `root_index`;

CREATE TABLE `root_index` (
  `uid` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) NOT NULL,
  `type` tinyint(1) unsigned NOT NULL,
  `fatherid` bigint(20) unsigned NOT NULL,
  `deleted` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `size` bigint(20) unsigned DEFAULT NULL,
  `mtime` timestamp NULL DEFAULT NULL COMMENT 'modification time',
  `ctime` timestamp NULL DEFAULT NULL COMMENT 'change attribs time',
  `atime` timestamp NULL DEFAULT NULL COMMENT 'last access time',
  `needtobackup` tinyint(1) NOT NULL DEFAULT '1',
  PRIMARY KEY (`uid`,`name`,`fatherid`),
  KEY `fatherid` (`fatherid`),
  KEY `type` (`type`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COMMENT='utf8_general_ci';

/*Data for the table `root_index` */

/*Table structure for table `tape_format` */

DROP TABLE IF EXISTS `tape_format`;

CREATE TABLE `tape_format` (
  `type` smallint(5) unsigned NOT NULL,
  `capacity` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

/*Data for the table `tape_format` */

insert  into `tape_format`(`type`,`capacity`) values 
(1,99750000000),
(2,199500000000),
(3,399000000000),
(4,798000000000),
(5,1496250000000),
(6,2493750000000),
(7,5985000000000),
(8,12768000000000),
(9,24937500000000);

/*Table structure for table `tapes` */

DROP TABLE IF EXISTS `tapes`;

CREATE TABLE `tapes` (
  `tape_no` bigint(20) unsigned NOT NULL COMMENT 'номер кассеты',
  `tape_format` smallint(5) unsigned NOT NULL COMMENT 'формат LTO',
  `pos_x` tinyint(3) unsigned NOT NULL COMMENT 'координата X на полке',
  `pos_y` tinyint(3) unsigned NOT NULL COMMENT 'координата Y на полке',
  `pos_z` tinyint(3) unsigned NOT NULL COMMENT 'координата Z на полке',
  `load_counter` smallint(5) unsigned NOT NULL COMMENT 'количество загрузок в привод',
  `first_use_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'дата первого использования',
  `broken` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT 'содержит ошибку, исключена из массива',
  PRIMARY KEY (`tape_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*Data for the table `tapes` */

insert  into `tapes`(`tape_no`,`tape_format`,`pos_x`,`pos_y`,`pos_z`,`load_counter`,`first_use_date`,`broken`) values 
(0,2,0,0,0,0,'2019-04-09 12:12:21',0);

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
