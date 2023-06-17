CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_table_type(
  p_table TEXT
) RETURNS TEXT AS
  $BODY$
    DECLARE
      query TEXT;
      data_type TEXT;
      schema_name TEXT;
      temp_table_name TEXT;
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN
          schema_name := (SELECT split_part(p_table, '.', 1));
          temp_table_name := (SELECT split_part(p_table, '.', 2));
      ELSE
        schema_name := 'public';
        temp_table_name := p_table;
      END IF;

      query := FORMAT(
        $i$
          SELECT data_type
          FROM information_schema.columns
          WHERE table_name = '%s'
            AND table_schema = '%s'
            AND column_name = 'effective'
        $i$,
        temp_table_name,
        schema_name
      );
      EXECUTE query INTO data_type;

      IF (SELECT data_type = 'tstzrange')
        THEN RETURN('interval');
      ELSIF (SELECT data_type = 'timestamp with time zone')
        THEN RETURN('event'); 
      ELSE
        RAISE EXCEPTION 'Invalid data type for effective';
        RETURN('unknown'); 
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;