CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
  p_schema_name text,
  p_table_name text,
  p_list_of_fields text,
  p_list_of_values text,
  p_search_fields text,
  p_search_values text,
  p_effective temporal_relationships.timeperiod,
  p_now temporal_relationships.time_endpoint
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_sql TEXT;
      v_rowcount INTEGER := 0;
      v_list_of_fields_to_insert TEXT;
      v_table_attr TEXT[];
      v_now temporal_relationships.time_endpoint := p_now;-- for compatiability with the previous version
      v_serial_key text := p_table_name || '_key';
      v_table text := p_schema_name || '.' || p_table_name;
      v_effective_start temporal_relationships.time_endpoint := LOWER(p_effective);
      v_keys INT[];
      v_keys_old INT[];
    BEGIN
      v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
      IF ARRAY_LENGTH(v_table_attr, 1) = 0
        THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
        RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');
      EXECUTE FORMAT(
        $u$
          WITH updt AS (
            UPDATE %s SET asserted = temporal_relationships.timeperiod_range(lower(asserted), %L, '[)')
            WHERE ( %s )=( %s )
              AND %L = LOWER(effective)
              AND UPPER(asserted) = 'infinity' 
              AND LOWER(asserted) < %L
            RETURNING %s
          )
          SELECT array_agg(%s) FROM updt
        $u$,  --end assertion period for the old record(s), if any
        v_table,
        v_now,
        p_search_fields,
        p_search_values,
        v_effective_start,
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
        v_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        v_table,
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
            UPDATE %s SET ( %s ) = (SELECT %s )
            WHERE ( %s ) = ( %s )
              AND effective = %L
              AND UPPER(asserted) = 'infinity'
          $uu$,  --update new assertion rage with new values
          v_table,
          p_list_of_fields,
          p_list_of_values,
          p_search_fields,
          p_search_values,
          p_effective
        );
      ELSE EXECUTE FORMAT(
      -- v_sql:=   
        $uu$
          UPDATE %s SET ( %s ) = ( SELECT %s )
          WHERE ( %s ) IN ( %s )
        $uu$,  --update new assertion rage with new values
        v_table,
        p_list_of_fields,
        p_list_of_values,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys, ','), 'NULL')
      );
        --  raise notice 'sql%', v_sql; 
      END IF;  
      GET DIAGNOSTICS v_rowcount := ROW_COUNT; 

      RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE; 
 
CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_search_fields TEXT,
  p_search_values TEXT,
  p_effective temporal_relationships.timeperiod
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_correction(
          p_schema_name,
          p_table_name,
          p_list_of_fields,
          p_list_of_values,
          p_search_fields,
          p_search_values,
          p_effective,
          now()
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql VOLATILE;