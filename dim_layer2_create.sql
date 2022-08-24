CREATE OR REPLACE PROCEDURE airflow_test.dim_layer2_create(project_name STRING, dimension_dataset STRING, dimension_table STRING, 
metadata_table STRING, batch_date STRING, output_dataset STRING,output_table STRING, other_details STRING)
  BEGIN
      /*
      DECLARE project_name STRING DEFAULT "";
      DECLARE dimension_dataset STRING DEFAULT "";
      DECLARE dimension_table STRING DEFAULT "";
      DECLARE metadata_table STRING DEFAULT "";
      DECLARE output_dataset STRING DEFAULT "";
      DECLARE output_table STRING DEFAULT "";
      DECLARE batch_date STRING DEFAULT "";


      DECLARE other_details STRING DEFAULT """{
          "is_dimension_type_segregation_req": "",
          "dimension_type_column": "",
          "dimension_type_value": "",
          "dimension_primary_key_column": "",
          "dimension_batch_date_column": "",
          "metadata_foreign_key_column": "",
          "metadata_key_column": "",
          "metadata_value_column": "",
          "metadata_batch_date_column": "",
          "control_table": "metadata.table"
        }"""; 
      */
      DECLARE attr STRING;
      DECLARE dimension_primary_key_column STRING;
      DECLARE metadata_foreign_key_column STRING;
      DECLARE metadata_key_column STRING;
      DECLARE metadata_value_column STRING;
      DECLARE metadata_batch_date_column STRING;
      DECLARE dimension_batch_date_column STRING;
      DECLARE is_dimension_type_segregation_req STRING;
      DECLARE dimension_type_column STRING;
      DECLARE dimension_type_value STRING;
      DECLARE control_table STRING;
      DECLARE col ARRAY<STRUCT<column_name STRING, data_type STRING>>;
      DECLARE col_metadata ARRAY<STRUCT<column_name STRING, data_type STRING>>;
      DECLARE output_table_exists BOOLEAN DEFAULT TRUE;

      
      SET dimension_primary_key_column = (SELECT LOWER(JSON_VALUE(other_details,"$.dimension_primary_key_column")));
      SET metadata_foreign_key_column = (SELECT LOWER(JSON_VALUE(other_details,"$.metadata_foreign_key_column")));
      SET metadata_key_column = (SELECT LOWER(JSON_VALUE(other_details,"$.metadata_key_column")));
      SET metadata_value_column = (SELECT LOWER(JSON_VALUE(other_details,"$.metadata_value_column")));
      SET metadata_batch_date_column = (SELECT LOWER(JSON_VALUE(other_details,"$.metadata_batch_date_column")));
      SET dimension_batch_date_column = (SELECT LOWER(JSON_VALUE(other_details,"$.dimension_batch_date_column")));
      SET is_dimension_type_segregation_req = (SELECT JSON_VALUE(other_details,"$.is_dimension_type_segregation_req"));
      SET dimension_type_column = (SELECT LOWER(JSON_VALUE(other_details,"$.dimension_type_column")));
      SET dimension_type_value = (SELECT LOWER(JSON_VALUE(other_details,"$.dimension_type_value")));
      SET control_table = (SELECT LOWER(JSON_VALUE(other_details,"$.control_table")));


EXECUTE IMMEDIATE FORMAT( """
SELECT IF(cnt>0,TRUE,FALSE) FROM 
    (SELECT COUNT(*) cnt FROM %s.%s.INFORMATION_SCHEMA.TABLES WHERE LOWER(table_name) IN (lower('%s')))
""", project_name, output_dataset, output_table
) into output_table_exists;

EXECUTE IMMEDIATE FORMAT( """
    SELECT STRING_AGG(concat(column_name, " ", data_type)) FROM %s.%s.INFORMATION_SCHEMA.COLUMNS WHERE LOWER(table_name) = lower('%s')
""", project_name, dimension_dataset, dimension_table
) into attr;

IF NOT output_table_exists
THEN
 EXECUTE IMMEDIATE FORMAT( """CREATE TABLE %s.%s.%s (%s) PARTITION BY %s OPTIONS (require_partition_filter = TRUE)""", 
 project_name, output_dataset, output_table, attr, dimension_batch_date_column);
END IF;

EXECUTE IMMEDIATE FORMAT( """
  WITH all_columns AS (
    SELECT column_name,data_type FROM %s.%s.INFORMATION_SCHEMA.COLUMNS 
    WHERE LOWER(table_name) IN ('%s') and LOWER(column_name) IN ('%s','%s','%s')
  )
  SELECT ARRAY_AGG(STRUCT(column_name, data_type)) AS col
  FROM all_columns""", 
project_name, dimension_dataset, metadata_table, metadata_key_column, metadata_value_column, metadata_foreign_key_column) INTO col;

---Flattenig for metadata starts
---Do partitioning on batch_date while writing the final output. don't forget.  
IF  is_dimension_type_segregation_req = "Y" THEN
      EXECUTE IMMEDIATE FORMAT("""
        CREATE TEMP TABLE metadata_temp_%s_%s( %s %s, %s %s, %s %s) AS 
        SELECT a.%s, a.%s, a.%s FROM
        (SELECT * FROM %s.%s.%s WHERE %s = '%s' and %s is not null) a join 
        (SELECT %s FROM %s.%s.%s b WHERE %s = '%s' and %s='%s') b 
        on a.%s=b.%s""", 
      dimension_table,regexp_replace(batch_date,"-","_"),
      col[OFFSET(0)].column_name, col[OFFSET(0)].data_type, col[OFFSET(1)].column_name, col[OFFSET(1)].data_type, col[OFFSET(2)].column_name, col[OFFSET(2)].data_type,
      col[OFFSET(0)].column_name, col[OFFSET(1)].column_name, col[OFFSET(2)].column_name,
      project_name, dimension_dataset, metadata_table, metadata_batch_date_column, batch_date, metadata_key_column,
      dimension_primary_key_column, project_name, dimension_dataset, dimension_table, dimension_batch_date_column, batch_date, dimension_type_column, dimension_type_value,
      metadata_foreign_key_column, dimension_primary_key_column);
ELSE
      EXECUTE IMMEDIATE FORMAT("""
        CREATE TEMP TABLE metadata_temp_%s_%s( %s %s, %s %s, %s %s) AS 
        SELECT %s, %s, %s FROM
        %s.%s.%s 
        WHERE %s = '%s' AND %s IS NOT NULL""",
      dimension_table,regexp_replace(batch_date,"-","_"),
      col[OFFSET(0)].column_name, col[OFFSET(0)].data_type, col[OFFSET(1)].column_name, col[OFFSET(1)].data_type, col[OFFSET(2)].column_name, col[OFFSET(2)].data_type,
      col[OFFSET(0)].column_name, col[OFFSET(1)].column_name, col[OFFSET(2)].column_name,
      project_name, dimension_dataset, metadata_table,
      metadata_batch_date_column, batch_date, metadata_key_column);
END IF;

EXECUTE IMMEDIATE FORMAT("""SELECT STRING_AGG(DISTINCT IF(alias IS NULL,REGEXP_REPLACE(LOWER(CONCAT('"',%s,'"'))," ","_"),concat(REGEXP_REPLACE(LOWER(CONCAT('"',%s,'"'))," ","_")," as ",alias)))
 FROM (SELECT * FROM metadata_temp_%s_%s) a join
(SELECT LOWER(column_name) colname, alias FROM %s.%s WHERE status = "active") b
on LOWER(a.%s) =b.colname""",
metadata_key_column,metadata_key_column,
dimension_table,regexp_replace(batch_date,"-","_"),
project_name, control_table, metadata_key_column ) INTO attr;

EXECUTE IMMEDIATE FORMAT("""SELECT ARRAY_AGG(STRUCT(column_name, data_type)) AS col_metadata FROM 
(SELECT column_name,data_type FROM %s.%s.INFORMATION_SCHEMA.COLUMNS WHERE table_name IN ('%s')
UNION ALL
select colname column_name,data_type from
  (SELECT distinct %s,"string" data_type FROM metadata_temp_%s_%s) a join
  (SELECT column_name, if(alias is not null, lower(alias), LOWER(column_name)) colname FROM %s.%s WHERE status = "active") b
 on a.%s=b.column_name)
""", 
project_name, dimension_dataset, dimension_table,
metadata_key_column, dimension_table,regexp_replace(batch_date,"-","_"),
project_name, control_table,
metadata_key_column) into col_metadata;

--creating asset dimension
IF  is_dimension_type_segregation_req = "Y" THEN
    EXECUTE IMMEDIATE FORMAT("""
        CREATE TEMP TABLE stage_%s_%s AS
          SELECT dim.*,dim_metadata.* EXCEPT(%s) FROM
          (SELECT * FROM %s.%s.%s b WHERE %s = '%s' AND %s='%s') dim
          LEFT OUTER JOIN
          (SELECT * FROM (SELECT %s, REGEXP_REPLACE(LOWER(%s)," ","_") %s, %s FROM metadata_temp_%s_%s)
        PIVOT(STRING_AGG(%s) FOR %s IN (%s))) dim_metadata
          ON dim.%s=dim_metadata.%s""", 
    dimension_table, regexp_replace(batch_date,"-","_"),
    metadata_foreign_key_column, 
    project_name, dimension_dataset, dimension_table, dimension_batch_date_column, batch_date, dimension_type_column, dimension_type_value,

    metadata_foreign_key_column, metadata_key_column, metadata_key_column, metadata_value_column, dimension_table, regexp_replace(batch_date,"-","_"),
    metadata_value_column, metadata_key_column, attr,
    dimension_primary_key_column, metadata_foreign_key_column);
ELSE
    EXECUTE IMMEDIATE FORMAT("""
        CREATE TEMP TABLE stage_%s_%s AS
          SELECT dim.*,dim_metadata.* except(%s) FROM
          (SELECT * FROM %s.%s.%s b WHERE %s = '%s') dim
          LEFT OUTER JOIN
          (SELECT * FROM (SELECT %s, REGEXP_REPLACE(LOWER(%s)," ","_") %s,%s FROM metadata_temp_%s_%s)
        PIVOT(STRING_AGG(%s) FOR %s IN (%s))) dim_metadata
          ON dim.%s=dim_metadata.%s""", 
    dimension_table, regexp_replace(batch_date,"-","_"),
    metadata_foreign_key_column, 
    project_name, dimension_dataset, dimension_table, dimension_batch_date_column, batch_date,

    metadata_foreign_key_column, metadata_key_column, metadata_key_column, metadata_value_column, dimension_table, regexp_replace(batch_date,"-","_"),
    metadata_value_column, metadata_key_column, attr,
    dimension_primary_key_column, metadata_foreign_key_column);
END IF;

CALL airflow_test.update_dest_table(project_name, FORMAT("stage_%s_%s",dimension_table, regexp_replace(batch_date,"-","_")), output_dataset, output_table, dimension_primary_key_column, col_metadata, dimension_batch_date_column, batch_date);

EXECUTE IMMEDIATE FORMAT("DROP TABLE metadata_temp_%s_%s", dimension_table, regexp_replace(batch_date,"-","_"));
EXECUTE IMMEDIATE FORMAT("DROP TABLE stage_%s_%s", dimension_table, regexp_replace(batch_date,"-","_"));

EXCEPTION WHEN ERROR THEN
  SELECT
    @@error.statement_text as statement_text,
    @@error.message as error_message,    
    @@error.formatted_stack_trace as stack_trace;
END
