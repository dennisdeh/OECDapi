-- Adminer 5.0.6 MariaDB 11.4.5-MariaDB dump

SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

USE `standard`;

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `table0`;
CREATE TABLE `table0` (
  `index` int(11) DEFAULT NULL,
  `value0` int(11) NOT NULL,
  `value1` int(11) NOT NULL,
  `filter column` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `table0` (`index`, `value0`, `value1`, `filter column`) VALUES
(0,	0,	2,	0),
(0,	1,	1,	1),
(2,	2,	2,	2),
(3,	2,	3,	0),
(4,	4,	3,	0),
(4,	1,	2,	1),
(5,	2,	3,	0),
(1,	2,	2,	3),
(1,	1,	1,	1),
(1,	0,	2,	0),
(1,	1,	1,	1),
(3,	2,	2,	2),
(4,	2,	3,	0),
(5,	4,	3,	0),
(5,	1,	2,	1),
(6,	2,	3,	0),
(2,	2,	2,	3),
(2,	1,	1,	1),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	2),
(1,	1,	2,	0),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	0),
(1,	1,	2,	3),
(1,	1,	2,	1),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	2),
(1,	1,	2,	0),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	0),
(1,	1,	2,	3),
(1,	1,	2,	1);

DROP TABLE IF EXISTS `table1`;
CREATE TABLE `table1` (
  `index` int(11) DEFAULT NULL,
  `value0` int(11) NOT NULL,
  `value1` int(11) NOT NULL,
  `filter column` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO `table1` (`index`, `value0`, `value1`, `filter column`) VALUES
(0,	0,	3,	0),
(0,	1,	1,	1),
(2,	2,	2,	2),
(3,	2,	2,	0),
(4,	4,	3,	0),
(4,	1,	2,	1),
(5,	2,	3,	0),
(1,	2,	2,	2),
(1,	1,	1,	1),
(1,	0,	2,	0),
(1,	1,	1,	1),
(3,	2,	2,	2),
(4,	2,	3,	0),
(5,	4,	3,	0),
(5,	1,	2,	1),
(6,	2,	3,	0),
(2,	2,	2,	3),
(2,	1,	1,	1),
(16,	1,	2,	0),
(18,	1,	2,	1),
(13,	1,	2,	2),
(1,	1,	2,	0),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	0),
(17,	1,	2,	1),
(1,	1,	2,	1),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	2),
(1,	1,	2,	0),
(1,	1,	2,	0),
(1,	1,	2,	1),
(1,	1,	2,	0),
(1,	1,	2,	3),
(1,	1,	2,	1);

-- 2025-03-21 23:02:07 UTC
