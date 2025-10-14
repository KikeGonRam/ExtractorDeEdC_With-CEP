/*
SQLyog Ultimate v11.11 (64 bit)
MySQL - 5.5.5-10.4.32-MariaDB : Database - extractor_estados
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`extractor_estados` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_spanish_ci */;

USE `extractor_estados`;

/*Table structure for table `solicitudes` */

DROP TABLE IF EXISTS `solicitudes`;

CREATE TABLE `solicitudes` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `archivo_nombre` varchar(255) NOT NULL,
  `archivo_tamano` bigint(20) unsigned DEFAULT NULL,
  `archivo_sha256` char(64) DEFAULT NULL,
  `salida_nombre` varchar(255) DEFAULT NULL,
  `salida_tamano` bigint(20) unsigned DEFAULT NULL,
  `salida_sha256` char(64) DEFAULT NULL,
  `banco` enum('banorte','bbva','santander','inbursa') NOT NULL,
  `empresa` varchar(160) NOT NULL,
  `solicitado_en` datetime NOT NULL DEFAULT current_timestamp(),
  `resultado` enum('xlsx','zip') NOT NULL,
  `estado` enum('ok','fail','processing') NOT NULL DEFAULT 'processing',
  `error` text DEFAULT NULL,
  `ip_cliente` varchar(45) DEFAULT NULL,
  `duracion_ms` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_fecha` (`solicitado_en`),
  KEY `idx_empresa` (`empresa`),
  KEY `idx_banco` (`banco`),
  KEY `idx_resultado` (`resultado`),
  KEY `idx_salida_sha256` (`salida_sha256`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

/*Data for the table `solicitudes` */

insert  into `solicitudes`(`id`,`archivo_nombre`,`archivo_tamano`,`archivo_sha256`,`salida_nombre`,`salida_tamano`,`salida_sha256`,`banco`,`empresa`,`solicitado_en`,`resultado`,`estado`,`error`,`ip_cliente`,`duracion_ms`) values (7,'LAMBASSA_JULIO 2025.pdf',141988,'d594a427eae22a98da6ff00d5a0bdb2943b2e3f815f4292fbcb7e250ed2623eb',NULL,NULL,NULL,'santander','SIN_EMPRESA','2025-09-12 09:45:44','xlsx','ok',NULL,NULL,NULL),(8,'DRENTELEX_ JULIO 2025 (1).pdf',644961,'e01f2abac8cfebe6a1e85c67412924cea59b1e6dff965c9fb458e45d8dc8840c',NULL,NULL,NULL,'bbva','SIN_EMPRESA','2025-09-12 13:22:46','xlsx','ok',NULL,NULL,NULL),(9,'MAYO IMBURSA      EdoCuenta_Inbursa.pdf',170463,'7b4736ffe4356eaffa59588ca56e64f733e5570ea085c5d2ec21161b6a607876',NULL,NULL,NULL,'inbursa','SIN_EMPRESA','2025-09-12 13:59:10','xlsx','ok',NULL,NULL,NULL),(10,'MAYO IMBURSA      EdoCuenta_Inbursa.pdf',170463,'7b4736ffe4356eaffa59588ca56e64f733e5570ea085c5d2ec21161b6a607876',NULL,NULL,NULL,'inbursa','SIN_EMPRESA','2025-09-12 13:59:13','zip','ok',NULL,NULL,NULL),(11,'MAYO IMBURSA      EdoCuenta_Inbursa.pdf',170463,'7b4736ffe4356eaffa59588ca56e64f733e5570ea085c5d2ec21161b6a607876','MAYO IMBURSA      EdoCuenta_Inbursa.xlsx',6921,'09a25e1471affeca1e18a004d3b6ca2b477690a82a4b061049dcb5b083e52eb0','inbursa','SIN_EMPRESA','2025-09-12 15:44:06','xlsx','ok',NULL,NULL,NULL),(12,'MAYO IMBURSA      EdoCuenta_Inbursa.pdf',170463,'7b4736ffe4356eaffa59588ca56e64f733e5570ea085c5d2ec21161b6a607876','MAYO IMBURSA      EdoCuenta_Inbursa_ceps.zip',62846,'90a8fedef0b27d28b7a8b1a9f5e125994b32d4388e5efc009c02f8aa036dcb3c','inbursa','SIN_EMPRESA','2025-09-12 15:44:58','zip','ok',NULL,NULL,NULL),(13,'LAMBASSA_JULIO 2025.pdf',141988,'d594a427eae22a98da6ff00d5a0bdb2943b2e3f815f4292fbcb7e250ed2623eb','LAMBASSA_JULIO 2025.xlsx',8096,'314ad7dc0406e63018d4feee535e8620d29a742e4f568bf61f73d9e86074ce01','santander','SIN_EMPRESA','2025-09-12 15:59:33','xlsx','ok',NULL,NULL,NULL),(14,'LAMBASSA_JULIO 2025.pdf',141988,'d594a427eae22a98da6ff00d5a0bdb2943b2e3f815f4292fbcb7e250ed2623eb','LAMBASSA_JULIO 2025.xlsx',8095,'b035f41944be0d2ce58cf0d07c1b061636fb7cc81b36103079ce1954ec2ead7d','santander','RFC','2025-09-12 16:15:30','xlsx','ok',NULL,NULL,NULL),(15,'DRENTELEX_ JULIO 2025 (1).pdf',644961,'e01f2abac8cfebe6a1e85c67412924cea59b1e6dff965c9fb458e45d8dc8840c','DRENTELEX_ JULIO 2025 (1).xlsx',9465,'4132009305d6997ac3f8765fcc46a405604e9243ded8c5a7a504e695ad619dcd','bbva','DRENTELEX SA DE CV','2025-09-15 10:33:43','xlsx','ok',NULL,NULL,NULL),(16,'DRENTELEX_ JULIO 2025 (1).pdf',644961,'e01f2abac8cfebe6a1e85c67412924cea59b1e6dff965c9fb458e45d8dc8840c','DRENTELEX_ JULIO 2025 (1).xlsx',9465,'4bd77cddfe57f68f549648f0b70f09990b627c3e9f17d6c7f4f7b69b8b6d3cee','bbva','DRENTELEX SA DE CV','2025-09-15 10:52:42','xlsx','ok',NULL,NULL,NULL),(17,'DRENTELEX_ JULIO 2025 (1).pdf',644961,'e01f2abac8cfebe6a1e85c67412924cea59b1e6dff965c9fb458e45d8dc8840c','DRENTELEX_ JULIO 2025 (1)_ceps.zip',761045,'58de63e3e5c92b753b641118b547eb2db7011e2e2de3ce46525a58ad8040218f','bbva','DRENTELEX SA DE CV','2025-09-15 11:01:34','zip','ok',NULL,NULL,NULL);

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
