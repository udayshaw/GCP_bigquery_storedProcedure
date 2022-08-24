CREATE OR REPLACE PROCEDURE airflow_test.update_dest_table(project_name STRING, source_table STRING,
dest_dataset STRING, dest_table STRING, primary_key STRING, col_metadata ARRAY<STRUCT<column_name STRING, data_type STRING>>, dimension_batch_date_column STRING, batch_date STRING)
  BEGIN

DECLARE new_cols ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE col_destTable ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE colnames ARRAY<STRING>;
DECLARE x INT64 DEFAULT 0;
DECLARE i INT64 DEFAULT 0;
DECLARE updateStatement STRING DEFAULT "";
DECLARE columnList STRING DEFAULT "";
DECLARE alter_string STRING;

/*******************
to get columns that are new to destination table
*******************/
EXECUTE IMMEDIATE FORMAT( """
  WITH all_data_columns AS ( SELECT column_name,data_type FROM %s.%s.INFORMATION_SCHEMA.COLUMNS WHERE table_name IN ('%s'))
  SELECT ARRAY_AGG(STRUCT(column_name, data_type)) AS col_destTable
  FROM all_data_columns
""",project_name,dest_dataset,dest_table) into col_destTable;
select col_metadata;
select col_destTable;
SET new_cols = (SELECT ARRAY_AGG(STRUCT(column_name,data_type)) FROM  
                    (SELECT column_name,data_type,count(*) FROM (
                       SELECT LOWER(col_metadata.column_name) column_name, LOWER(col_metadata.data_type) data_type FROM UNNEST(col_metadata) col_metadata
                       UNION ALL
                       SELECT LOWER(col_destTable.column_name) column_name, LOWER(col_destTable.data_type) data_type FROM UNNEST(col_destTable) col_destTable
                    ) GROUP BY column_name,data_type HAVING count(*) =1));

--taking total number of new columns
SET x=(SELECT ARRAY_LENGTH(new_cols));
SET alter_string = (SELECT STRING_AGG(CONCAT(" add column ",col.column_name," ",col.data_type)) FROM UNNEST(new_cols) col);

IF x>0 THEN
  EXECUTE IMMEDIATE FORMAT ("ALTER TABLE %s.%s.%s %s", project_name, dest_dataset, dest_table, alter_string);
END IF;

EXECUTE IMMEDIATE FORMAT("""
SELECT STRING_AGG(column_name) colist FROM %s.%s.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_name = '%s'""",project_name, dest_dataset, dest_table) into columnList;

EXECUTE IMMEDIATE FORMAT("SELECT split('%s',',')",columnList) into colnames;

SET x=(SELECT ARRAY_LENGTH(colnames));
SET i=0;
LOOP
   --Generating update statement for Merge
   IF colnames[OFFSET(i)] != primary_key THEN
     SET updateStatement=(SELECT CONCAT(updateStatement,(SELECT CONCAT(colnames[OFFSET(i)],"=b.",colnames[OFFSET(i)],IF(i<x-1,",\n","\n")))));
   END IF;
   SET i = i + 1;
  --SELECT i;
  IF i >= x THEN
    LEAVE;
  END IF;
END LOOP;

--SELECT updateStatement;


EXECUTE IMMEDIATE FORMAT("""
MERGE %s.%s.%s a USING %s b ON a.%s=b.%s and a.%s="%s"
WHEN MATCHED THEN
  UPDATE SET 
  %s
WHEN NOT MATCHED THEN
  INSERT (%s) VALUES (%s);
  """,project_name, dest_dataset, dest_table, source_table, primary_key, primary_key, 
  dimension_batch_date_column, batch_date,
  updateStatement, columnList, columnList);
 
EXCEPTION WHEN ERROR THEN
  SELECT
    @@error.statement_text as statement_text,
    @@error.message as error_message,    
    @@error.formatted_stack_trace as stack_trace;
END