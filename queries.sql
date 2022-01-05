CREATE DATABASE samiti;
---------------------------------------------------------------
USE samiti; 
---------------------------------------------------------------
CREATE TABLE user
  (
     userid   INT(10) NOT NULL,
     username VARCHAR(255)  NOT NULL,
     PRIMARY KEY (username)
  ); 
---------------------------------------------------------------
INSERT INTO user VALUES (1, 'Dilip Tripathi');
INSERT INTO user VALUES (2, 'Dinbandhu Tailor');
INSERT INTO user VALUES (3, 'Sunil Joshi');
INSERT INTO user VALUES (4, 'Chandan Singh');
INSERT INTO user VALUES (5, 'Kamal Rawal');
INSERT INTO user VALUES (6, 'Lalit Samdani');
INSERT INTO user VALUES (7, 'Lalit Gurjur');
INSERT INTO user VALUES (8, 'Mukesh Samdani');
INSERT INTO user VALUES (9, 'Mahesh Sharma');
INSERT INTO user VALUES (10, 'Naresh Tailor');
INSERT INTO user VALUES (11, 'Ram Purohit');
INSERT INTO user VALUES (12, 'Rajesh Tailor');
INSERT INTO user VALUES (13, 'Dinesh Joshi');
INSERT INTO user VALUES (14, 'Subash Moghe');
INSERT INTO user VALUES (15, 'Suresh Tailor');
---------------------------------------------------------------
CREATE TABLE interest
  (
     datemonth    DATE NOT NULL,
     interestrate FLOAT(10, 2) NOT NULL
  );  
---------------------------------------------------------------
INSERT INTO  interest VALUES ('2020-01-01','1.00');
INSERT INTO  interest VALUES ('2020-02-01','1.00');
INSERT INTO  interest VALUES ('2020-03-01','1.00');
INSERT INTO  interest VALUES ('2020-04-01','1.00');
INSERT INTO  interest VALUES ('2020-05-01','1.00');
INSERT INTO  interest VALUES ('2020-06-01','1.00');
INSERT INTO  interest VALUES ('2020-07-01','1.00');
INSERT INTO  interest VALUES ('2020-08-01','1.00');
INSERT INTO  interest VALUES ('2020-09-01','1.00');
INSERT INTO  interest VALUES ('2020-10-01','1.00');
INSERT INTO  interest VALUES ('2020-11-01','1.00');
INSERT INTO  interest VALUES ('2020-12-01','1.00');
INSERT INTO  interest VALUES ('2021-01-01','1.00');
INSERT INTO  interest VALUES ('2021-02-01','1.00');
INSERT INTO  interest VALUES ('2021-03-01','1.00');
INSERT INTO  interest VALUES ('2021-04-01','1.00');
INSERT INTO  interest VALUES ('2021-05-01','1.00');
INSERT INTO  interest VALUES ('2021-06-01','1.00');
INSERT INTO  interest VALUES ('2021-07-01','1.00');
INSERT INTO  interest VALUES ('2021-08-01','1.00');
INSERT INTO  interest VALUES ('2021-09-01','1.00');
INSERT INTO  interest VALUES ('2021-10-01','1.00');
INSERT INTO  interest VALUES ('2021-11-01','1.00');
INSERT INTO  interest VALUES ('2021-12-01','1.00');
---------------------------------------------------------------
CREATE TABLE monthlycontractbalance
  (
     user_id                int NOT NULL,
     mcb_datemonth          DATE NOT NULL,
     outstanding_debt       BIGINT NOT NULL,
     share_amount           INT NOT NULL,
     loan_installment       BIGINT NOT NULL,
     interest_amount        INT NOT NULL,
     cash_collected         BIGINT NOT NULL,
     debit_balance          BIGINT NOT NULL,
     new_loan_amount        BIGINT NOT NULL,
     total_outstanding_debt BIGINT NOT NULL,
     update_datetime        timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (user_id, mcb_datemonth)
  ); 
---------------------------------------------------------------
INSERT INTO monthlycontractbalance VALUES (1,'2020-01-01',149000,200,2000,1490,3690,147000,0,147000,now();
INSERT INTO monthlycontractbalance VALUES (2,'2020-01-01',225000,200,10000,2250,12450,215000,0,215000,now());
INSERT INTO monthlycontractbalance VALUES (3,'2020-01-01',41000,200,0,410,610,41000,0,41000,now());
INSERT INTO monthlycontractbalance VALUES (4,'2020-01-01',81000,200,0,810,1010,81000,1010,82010,now());
INSERT INTO monthlycontractbalance VALUES (5,'2020-01-01',33000,200,2000,330,2530,31000,0,31000,now());
INSERT INTO monthlycontractbalance VALUES (6,'2020-01-01',25000,200,0,250,450,25000,0,25000,now());
INSERT INTO monthlycontractbalance VALUES (7,'2020-01-01',200000,200,2000,2000,4200,198000,0,198000,now());
INSERT INTO monthlycontractbalance VALUES (8,'2020-01-01',34000,200,27000,340,27540,7000,0,7000,now());
INSERT INTO monthlycontractbalance VALUES (9,'2020-01-01',120000,200,0,1200,1400,120000,75000,195000,now());
INSERT INTO monthlycontractbalance VALUES (10,'2020-01-01',83000,200,3000,830,4030,80000,0,80000,now());
INSERT INTO monthlycontractbalance VALUES (11,'2020-01-01',20000,200,0,200,400,20000,0,20000,now());
INSERT INTO monthlycontractbalance VALUES (12,'2020-01-01',138000,200,1000,1380,2580,137000,0,137000,now());
INSERT INTO monthlycontractbalance VALUES (13,'2020-01-01',30000,200,0,300,500,30000,0,30000,now());
INSERT INTO monthlycontractbalance VALUES (14,'2020-01-01',202000,200,10000,2020,12220,192000,0,192000,now());
INSERT INTO monthlycontractbalance VALUES (15,'2020-01-01',50000,200,3000,500,3700,47000,0,47000,now());

INSERT INTO monthlycontractbalance VALUES (12,'2020-11-01',142260,200,1260,1423,2883,141000,0,141000,now());
INSERT INTO monthlycontractbalance VALUES (12,'2020-12-01',141000,200,1000,1410,2610,141000,0,141000,now());
---------------------------------------------------------------
CREATE TABLE samitibank
  (
     sb_datemonth         DATE NOT NULL,
     total_cashcollected  BIGINT NOT NULL,
     total_newloanamt     BIGINT NOT NULL,
     carry_balance        INT,
     to_caryfwd_balance   INT,
     cashier_amt          INT NOT NULL,
     created_datetime     timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (sb_datemonth)
  ); 
---------------------------------------------------------------
INSERT INTO samitibank VALUES ('2020-01-01',77310,76010,505,805,1000,now());
INSERT INTO samitibank VALUES ('2020-02-01',79470,76020,805,4255,0,now());
INSERT INTO samitibank VALUES ('2020-03-01',40110,44030,4255,335,0,now());
INSERT INTO samitibank VALUES ('2020-04-01',0,0,0,0,0,now());
INSERT INTO samitibank VALUES ('2020-05-01',0,0,0,0,0,now());
INSERT INTO samitibank VALUES ('2020-06-01',0,0,0,0,0,now());
INSERT INTO samitibank VALUES ('2020-07-01',71304,66200,335,5439,0,now());
INSERT INTO samitibank VALUES ('2020-08-01',79748,85000,5439,187,0,now());
INSERT INTO samitibank VALUES ('2020-09-01',80925,81000,187,112,0,now());
INSERT INTO samitibank VALUES ('2020-10-01',49713,49760,112,65,0,now());
INSERT INTO samitibank VALUES ('2020-11-01',60163,60000,65,228,0,now());
INSERT INTO samitibank VALUES ('2020-12-01',64992,65000,228,219,0,now()); #1Rs subtract for sattlement 

INSERT INTO samitibank VALUES ('2021-12-01',100199,100000,1314,1513,2000,now());            
---------------------------------------------------------------
SELECT 
    mcb_datemonth,
    SUM(cash_collected),
    SUM(new_loan_amount)
FROM
    monthlycontractbalance
GROUP BY 
    mcb_datemonth
---------------------------------------------------------------
SELECT 
    mcb_datemonth,
    SUM(share_amount),
    SUM(loan_installment),
    SUM(interest_amount),
    SUM(cash_collected),
    SUM(new_loan_amount)
FROM
    monthlycontractbalance
GROUP BY 
    mcb_datemonth
---------------------------------------------------------------
SELECT user_id,
       mcb_datemonth,
       outstanding_debt,
       share_amount,
       loan_installment,
       interest_amount,
       cash_collected,
       debit_balance,
       new_loan_amount,
       total_outstanding_debt.
       update_datetime
FROM   monthlycontractbalance 
----------------------------------------------------------------
ALTER TABLE monthlycontractbalance ADD COLUMN update_datetime DATETIME;
----------------------------------------------------------------
--backup  table
CREATE TABLE mcb_backup AS SELECT * FROM monthlycontractbalance;
----------------------------------------------------------------
--previous month date
SELECT last_day('2021-01-01' - interval 2 month) + interval 1 day as previoudmonth
FROM monthlycontractbalance;
----------------------------------------------------------------