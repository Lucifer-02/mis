## Setup users
### Users

- misadmin: hold all privileges.
- data_stage: IT use with this user, privileges: select, insert, create table.
- mis_dev: to development.


## Setup tables

```sql
SELECT trunc(CURRENT_DATE) FROM dual;

CREATE TABLE FLAG_TBL(
        SURROGATE_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        UPDATE_TIME TIMESTAMP,
        RECORD_CREATION_DATE VARCHAR2(256),
        TABLE_NAME VARCHAR2(256),
        RERUN_FLAG NUMBER(1),
        DONE NUMBER(1)
);

DROP TABLE MIS_DEV.FLAG_TBL;

INSERT INTO MIS_DEV.FLAG_TBL
("PARTITION", DATE8, TABLE_NAME, DONE, "ROWS")
VALUES(CURRENT_DATE, '31-12-2023', 'test_table', 0, 2);

SELECT * FROM FLAG_TBL WHERE TRUNC("PARTITION") = TRUNC(CURRENT_DATE);
SELECT * FROM FLAG_TBL;

SELECT * FROM mis_dev.FLAG_TBL;
SELECT * FROM mis_dev.test_table;


CREATE TABLE mis_dev.test_table (
        col1 NUMBER,
        date8 varchar2(256)
);

DROP TABLE mis_dev.test_table;

INSERT INTO mis_dev.test_table (col1,date8)
VALUES (112, '30-12-2023');

INSERT ALL
INTO mis_dev.test_table  (col1,date8) VALUES (1,'31-12-2023')
INTO mis_dev.test_table  (col1,date8) VALUES (121,'31-12-2023')
SELECT * FROM dual;

INSERT ALL
INTO mis_dev.test_table  (col1,date8) VALUES (123,'31-10-2023')
INTO mis_dev.test_table  (col1,date8) VALUES (456,'31-10-2023')
SELECT * FROM dual;

-- DO THIS TO TEST THE TRIGGER
INSERT INTO MIS_DEV.FLAG_TBL
("PARTITION", DATE8, TABLE_NAME, DONE, "ROWS")
VALUES(CURRENT_DATE, '31-12-2023', 'test_table', 1, 2);
```

## Setup trigger
```sql


BEGIN
        dbms_scheduler.create_job(
                job_name        => 'my_script',
                job_type        => 'EXECUTABLE',
                job_action      => '/usr/bin/python3',
                number_of_arguments => 1
        );
        dbms_scheduler.set_job_argument_value(
                job_name => 'my_script',
                argument_position => 1,
                argument_value => '/home/oracle/hoangnlv/capture_new_records.py'
        );
END;

BEGIN
        dbms_scheduler.run_job('my_script');
END;



CREATE OR REPLACE PROCEDURE get_new_records
( col1 IN VARCHAR2,
  col2 IN VARCHAR2)
IS
        PRAGMA AUTONOMOUS_TRANSACTION; -- Mark the procedure as an autonomous transaction
BEGIN
    -- Set the first argument
    dbms_scheduler.drop_job(job_name => 'run_script');
        dbms_scheduler.create_job(
                job_name        => 'run_script',
                job_type        => 'EXECUTABLE',
                job_action      => '/home/oracle/hoangnlv/capture_new_records.py',
                number_of_arguments => 2
        );
    DBMS_SCHEDULER.set_job_argument_value(
        job_name        => 'run_script',
        argument_position => 1,
        argument_value  => col1
    );
    DBMS_SCHEDULER.set_job_argument_value(
        job_name        => 'run_script',
        argument_position => 2,
        argument_value  => col2
    );
    -- Run the job
    DBMS_SCHEDULER.run_job('run_script');
 END;

CREATE OR REPLACE TRIGGER new_records_trigger
AFTER INSERT ON mis_dev.flag_tbl
FOR EACH ROW
BEGIN
        get_new_records(:NEW.date8, :NEW.table_name);
END;

DROP TRIGGER new_records_trigger;
```
