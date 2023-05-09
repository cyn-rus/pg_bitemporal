CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_search_fields TEXT,
  p_search_values TEXT,
  p_now temporal_relationships.time_endpoint
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_list_of_fields_to_insert TEXT;
      v_table_attr TEXT[];
      v_now temporal_relationships.time_endpoint := p_now;-- for compatiability with the previous version
      v_serial_key TEXT;
      v_keys INT[];
      v_keys_old INT[];
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN v_serial_key := (SELECT split_part(p_table, '.', 2) || '_key');
      ELSE v_serial_key := p_table || '_key';
      END IF;

      v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(p_table);
      IF ARRAY_LENGTH(v_table_attr, 1) = 0
        THEN RAISE EXCEPTION 'Empty list of fields for a table: %', p_table; 
        RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');
      EXECUTE FORMAT(
        $u$
          WITH updt AS (
            UPDATE %s SET asserted = temporal_relationships.timeperiod_range(lower(asserted), %L, '[)')
            WHERE ( %s )=( %s )
              AND UPPER(asserted) = 'infinity' 
              AND LOWER(asserted) < %L
            RETURNING %s
          )
          SELECT array_agg(%s) FROM updt
        $u$,  --end assertion period for the old record(s), if any
        p_table,
        v_now,
        p_search_fields,
        p_search_values,
        v_now,
        v_serial_key,
        v_serial_key
      ) INTO v_keys_old;
 --       raise notice 'sql%', v_sql;  

      -- v_sql:=
      EXECUTE FORMAT(
        $i$
          WITH inst AS (
            INSERT INTO %s ( %s, effective, asserted )
              SELECT %s, effective, temporal_relationships.timeperiod_range(upper(asserted), 'infinity', '[)')
              FROM %s
              WHERE ( %s ) IN ( %s ) 
            RETURNING %s
          )
          SELECT array_agg(%s) FROM inst  --insert new assertion rage with old values where applicable 
        $i$,
        p_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        p_table,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys_old, ','), 'NULL'),
        v_serial_key,
        v_serial_key
      ) INTO v_keys;
      --raise notice 'sql%', v_sql;  

      --raise notice 'sql%', v_sql;
      IF COALESCE(ARRAY_TO_STRING(v_keys_old, ',')) IS NULL
        THEN EXECUTE FORMAT(
          $uu$
            UPDATE %s SET ( %s ) = ( SELECT %s )
            WHERE ( %s ) = ( %s )
              AND UPPER(asserted) = 'infinity'
          $uu$,  --update new assertion rage with new values
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_search_fields,
          p_search_values
        );
      ELSE EXECUTE FORMAT(
      -- v_sql:=   
        $uu$
          UPDATE %s SET ( %s ) = ( SELECT %s )
          WHERE ( %s ) IN ( %s )
        $uu$,  --update new assertion rage with new values
        p_table,
        p_list_of_fields,
        p_list_of_values,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys, ','), 'NULL')
      );
        -- raise notice 'sql%', v_sql; 
      END IF;  
      GET DIAGNOSTICS v_rowcount := ROW_COUNT; 

      RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE; 
 
CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_search_fields TEXT,
  p_search_values TEXT
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_correction(
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_search_fields,
          p_search_values,
          now()
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE;