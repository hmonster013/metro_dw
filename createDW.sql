drop schema if exists `dw` ;
DROP TABLE if exists `dw`.`product`;
DROP TABLE if exists `dw`.`supplier`;
DROP TABLE if exists `dw`.`customer`;
DROP TABLE if exists `dw`.`date`;
DROP TABLE if exists `dw`.`store`;
DROP TABLE if exists `dw`.`sales`;
commit;
CREATE SCHEMA `dw` ;

CREATE TABLE `dw`.`product` (
  `product_id` VARCHAR(6) NOT NULL,
  `product_name` VARCHAR(30) NOT NULL,
  `price` DOUBLE(5,2) NULL,
  PRIMARY KEY (`product_id`));
  
  CREATE TABLE `dw`.`supplier` (
  `supplier_id` VARCHAR(5) NOT NULL,
  `supplier_name` VARCHAR(30) NOT NULL,
  PRIMARY KEY (`supplier_id`));

CREATE TABLE `dw`.`customer` (
  `customer_id` VARCHAR(4) NOT NULL,
  `customer_name` VARCHAR(30) NOT NULL,
  PRIMARY KEY (`customer_id`));
  
CREATE TABLE `dw`.`date` (
  `time_id` VARCHAR(8) NOT NULL,
  `t_date` DATE NOT NULL,
  `weekend` TINYINT(1) NOT NULL,
  `half_of_year` TINYTEXT NOT NULL,
  `month` TINYTEXT NOT NULL,
  `quarter` VARCHAR(2) NOT NULL,
  `year` YEAR(4) NOT NULL,
  PRIMARY KEY (`time_id`));
  
CREATE TABLE `dw`.`store` (
  `store_id` VARCHAR(4) NOT NULL,
  `store_name` VARCHAR(20) NOT NULL,
  PRIMARY KEY (`store_id`));
  
CREATE TABLE `dw`.`sales` (
  `transaction_id` DOUBLE(8,0) NOT NULL,
  `product_id` VARCHAR(6) NOT NULL,
  `customer_id` VARCHAR(4) NOT NULL,
  `time_id` VARCHAR(8) NOT NULL,
  `store_id` VARCHAR(4) NOT NULL,
  `supplier_id` VARCHAR(5) NOT NULL,
  `quantity` DOUBLE(3,0) NOT NULL,
  `total_sale` DOUBLE NOT NULL,
  PRIMARY KEY (`transaction_id`),
  CONSTRAINT `product_id`
    FOREIGN KEY (`product_id`)
    REFERENCES `dw`.`product` (`product_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `customer_id`
    FOREIGN KEY (`customer_id`)
    REFERENCES `dw`.`customer` (`customer_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `time_id`
    FOREIGN KEY (`time_id`)
    REFERENCES `dw`.`date` (`time_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `store_id`
    FOREIGN KEY (`store_id`)
    REFERENCES `dw`.`store` (`store_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `supplier_id`
    FOREIGN KEY (`supplier_id`)
    REFERENCES `dw`.`supplier` (`supplier_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);