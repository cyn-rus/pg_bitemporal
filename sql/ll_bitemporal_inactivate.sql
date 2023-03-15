CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_inactivate(
  p_schema_name text,
  p_table_name text,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT, --  search values
  p_effective temporal_relationships.timeperiod, -- inactive starting
  p_asserted temporal_relationships.timeperiod -- will be asserted
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_sql text;
      v_rowcount INTEGER := 0;
      v_list_of_fields_to_insert text := ' ';
      v_list_of_fields_to_insert_excl_effective text;
      v_table_attr text[];
      v_now timestamptz := now(); -- so that we can reference this time
      v_keys int[];
      v_keys_old  int[];
      v_serial_key text := p_table_name || '_key';
      v_table text := p_schema_name || '.' || p_table_name;
    BEGIN 
    IF LOWER(p_asserted) < v_now::date --should we allow this precision?...
      OR UPPER(p_asserted) < 'infinity'
      THEN RAISE EXCEPTION 'Asserted interval starts in the past or has a finite end: %', p_asserted; 
        RETURN v_rowcount;
    END IF;
  /* IF (bitemporal_internal.ll_check_bitemporal_update_conditions(p_table 
                                                        ,p_search_fields 
                                                        ,p_search_values
                                                        ,p_effective)  =0 )
  THEN RAISE EXCEPTION'Nothing to inactivate: % = %, effective %', p_search_fields, p_search_values, p_effective; 
    RETURN v_rowcount;
  END IF;   
  */
    v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
    IF ARRAY_LENGTH(v_table_attr, 1) = 0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
        RETURN v_rowcount;
    END IF;
    v_list_of_fields_to_insert_excl_effective := ARRAY_TO_STRING(v_table_attr, ',', '');
    v_list_of_fields_to_insert := v_list_of_fields_to_insert_excl_effective || ',effective';
    
  --end assertion period for the old record(s)

    EXECUTE FORMAT(
      $u$
        WITH updt AS (
          UPDATE %s 
          SET asserted = temporal_relationships.timeperiod(LOWER(asserted), lower(%L::temporal_relationships.timeperiod))
          WHERE ( %s )=( %s ) AND (temporal_relationships.is_overlaps(effective, %L)
              OR temporal_relationships.is_meets(effective, %L)
              OR temporal_relationships.has_finishes(effective, %L))
            AND (temporal_relationships.is_overlaps(asserted, %L) --now() <@ asserted
              OR temporal_relationships.has_finishes(asserted, %L)) RETURNING %s)
        SELECT array_agg(%s) FROM updt
      $u$,  
      v_table,
      p_asserted,
      p_search_fields,
      p_search_values,
      p_effective,
      p_effective,
      p_effective,
      p_asserted,
      p_asserted,
      v_serial_key,
      v_serial_key
    ) INTO v_keys_old;
            
  --insert new assertion range with old values and effective-ended
  
    EXECUTE FORMAT(
      $i$
        INSERT INTO %s (%s, effective, asserted)
        SELECT %s, temporal_relationships.timeperiod(LOWER(effective), LOWER(%L::temporal_relationships.timeperiod)), %L
        FROM %s
        WHERE ( %s ) in ( %s )
      $i$,
      v_table,
      v_list_of_fields_to_insert_excl_effective,
      v_list_of_fields_to_insert_excl_effective,
      p_effective,
      p_asserted,
      v_table,
      v_serial_key,
      COALESCE(array_to_string(v_keys_old,','),'NULL')
    );

    GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 
    RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_inactivate(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_effective temporal_relationships.timeperiod -- inactive starting
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_inactivate(
          p_schema_name,
          p_table_name,
          p_search_fields,
          p_search_values,
          p_effective,
          temporal_relationships.timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_inactivate(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT  --  search values
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_inactivate(
          p_schema_name,
          p_table_name,
          p_search_fields,
          p_search_values,
          p_effective,
          temporal_relationships.timeperiod(now(), 'infinity'),
          temporal_relationships.timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;