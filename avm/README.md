## Configuration

- Script: `/home/oracle/hoangnlv/capture_new_records.py`
- Output: `/home/oracle/hoangnlv/new_records.csv`
- Trigger log: `/home/oracle/hoangnlv/trigger.log`
- Trigger setup:

```sql
CREATE OR REPLACE PROCEDURE get_new_records
( col1 IN VARCHAR2,
  col2 IN VARCHAR2)
IS
    PRAGMA AUTONOMOUS_TRANSACTION; -- Mark the procedure as an autonomous transaction
BEGIN
    -- Set the first argument
  dbms_scheduler.drop_job(job_name => 'run_script');
    dbms_scheduler.create_job(
        job_name    => 'run_script',
        job_type    => 'EXECUTABLE',
        job_action  => '/home/oracle/hoangnlv/capture_new_records.py',
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
```

> After inserting new records into a table, the trigger will run the `capture_new_records.py` script when the user inserts metadata record into the `MIS_DEV.FLAG_TBL` table to capture new records and save to `new_records.csv`.

## Example

Assume a user inserts into `MIS_DEV.TEST_TABLE` like this:
```sql
INSERT ALL
INTO MIS_DEV.TEST_TABLE (col,date8) VALUES (1, '31-12-2023')
INTO MIS_DEV.TEST_TABLE (col,date8) VALUES (2, '31-12-2023')
SELECT * FROM dual;
```
Then the user has to insert into the `MIS_DEV.FLAG_TBL` table:
```sql
INSERT INTO MIS_DEV.FLAG_TBL
("PARTITION", DATE8, TABLE_NAME, DONE, "ROWS")
VALUES(CURRENT_DATE, '31-12-2023', 'test_table', 1, 2);
```
The trigger will query from the corresponding table with the new metadata record above to get new records. This is the content of the output file:

```csv
COL1,DATE8
1,31-12-2023
2,31-12-2023
```