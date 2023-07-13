CREATE OR REPLACE PROCEDURE bitemporal_internal.ll_bitemporal_fk_constraint(
  parent_table TEXT,
  parent_business_key TEXT,
  child_table TEXT,
  child_business_key TEXT
) $BODY$
    DECLARE
      trigger_func_name TEXT;
      p_temp_table_name TEXT;
      p_temp_schema_name TEXT;
      c_temp_table_name TEXT;
      c_temp_schema_name TEXT;
    BEGIN
      IF (SELECT source_table LIKE '%.%') THEN
        p_temp_schema_name := (SELECT split_part(parent_table, '.', 1));
        p_temp_table_name := (SELECT split_part(parent_table, '.', 2));
        c_temp_schema_name := (SELECT split_part(child_table, '.', 1));
        c_temp_table_name := (SELECT split_part(child_table, '.', 2));
        trigger_func_name := p_temp_schema_name || '_' || p_temp_table_name || '_id_fk_validate';
        trigger_name := c_temp_schema_name || '_' || c_temp_table_name || '_' || child_business_key || '_fk';
      ELSE
        p_temp_schema_name := 'public';
        p_temp_table_name := parent_table;
        c_temp_schema_name := 'public';
        c_temp_table_name := child_table;
        trigger_func_name := source_table || '_id_fk_validate';
        trigger_name := child_table || '_' || child_business_key || '_fk';
      END IF;

      EXECUTE FORMAT(
        $t$
          CREATE TRIGGER %s
          BEFORE INSERT OR UPDATE ON %s
          FOR EACH ROW
          EXECUTE PROCEDURE %s
        $t$,
        trigger_name,
        child_table,
        trigger_func_name
      ); 
    END
  $BODY
LANGUAGE plgpsql;