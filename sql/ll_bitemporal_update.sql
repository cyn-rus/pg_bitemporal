CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_update(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_list_of_fields text, -- fields to update
  p_list_of_values TEXT,  -- values to update with
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_effective temporal_relationships.timeperiod,  -- effective range of the update
  p_asserted temporal_relationships.timeperiod  -- assertion for the update
) 
RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INTEGER:=0;
      v_list_of_fields_to_insert text:=' ';
      v_list_of_fields_to_insert_excl_effective TEXT;
      v_table_attr TEXT[];
      v_serial_key text:=p_table_name||'_key';
      v_table text:=p_schema_name||'.'||p_table_name;
      v_keys_old INT[];
      v_keys INT[];
      v_now timestamptz:=now();-- so that we can reference this time
    BEGIN 
    IF lower(p_asserted)<v_now::date --should we allow this precision?...
      OR upper(p_asserted)< 'infinity'
    THEN RAISE EXCEPTION'Asserted interval starts in the past or has a finite end: %', p_asserted; 
      RETURN v_rowcount;
    END IF;  

    v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(v_table);
    IF array_length(v_table_attr,1)=0
    THEN RAISE EXCEPTION 'Empty list of fields for a table: %', v_table; 
      RETURN v_rowcount;
    END IF;
    v_list_of_fields_to_insert_excl_effective:= ARRAY_TO_STRING(v_table_attr, ',','');
    v_list_of_fields_to_insert:= v_list_of_fields_to_insert_excl_effective||',effective';

--end assertion period for the old record(s)

    EXECUTE FORMAT(
      $u$
        WITH updt AS (
          UPDATE %s SET asserted = temporal_relationships.timeperiod(LOWER(asserted), LOWER(%L::temporal_relationships.timeperiod))
          WHERE ( %s )=( %s ) AND
            (temporal_relationships.is_overlaps(effective, %L)
              OR temporal_relationships.is_meets(effective::temporal_relationships.timeperiod, %L)
              OR temporal_relationships.has_finishes(effective::temporal_relationships.timeperiod, %L))
            AND now()<@ asserted
          RETURNING %s
        )
        SELECT array_agg(%s)
        FROM updt
      $u$,
      v_table,
      p_asserted,
      p_search_fields,
      p_search_values,
      p_effective,
      p_effective,
      p_effective,
      v_serial_key,
      v_serial_key
    ) INTO v_keys_old;

 --insert new assertion rage with old values and effective-ended
    EXECUTE FORMAT(
      $i$
        INSERT INTO %s ( %s, effective, asserted )
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
      COALESCE(ARRAY_TO_STRING(v_keys_old,','), 'NULL')
    );


---insert new assertion rage with old values and new effective range
 
    EXECUTE FORMAT(
      $i$
        WITH inst AS (
          INSERT INTO %s ( %s, effective, asserted )
            SELECT %s ,%L, %L
            FROM %s
            WHERE ( %s ) IN (%s )
          RETURNING %s
        )
        SELECT array_agg(%s)
        FROM inst
      $i$,
      v_table,
      v_list_of_fields_to_insert_excl_effective,
      v_list_of_fields_to_insert_excl_effective,
      p_effective,
      p_asserted,
      v_table,
      v_serial_key,
      COALESCE(ARRAY_TO_STRING(v_keys_old,','), 'NULL'),
      v_serial_key,
      v_serial_key
    ) INTO v_keys;

--update new record(s) in new assertion rage with new values                                  
                                  
    EXECUTE FORMAT(
--v_sql :=
      $u$
        UPDATE %s SET (%s) =
          (SELECT %s) 
          WHERE ( %s ) IN ( %s )
      $u$,
      v_table,
      p_list_of_fields,
      p_list_of_values,
      v_serial_key,
      COALESCE(ARRAY_TO_STRING(v_keys,','), 'NULL')
    );
          
    GET DIAGNOSTICS v_rowcount:=ROW_COUNT;  

    RETURN v_rowcount;
    END;    
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_update(
  p_schema_name TEXT,
  p_table_name TEXT,
  p_list_of_fields text, -- fields to update
  p_list_of_values TEXT,  -- values to update with
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_effective temporal_relationships.timeperiod  -- effective range of the update
)
RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_update(
          p_schema_name,
          p_table_name,
          p_list_of_fields,
          p_list_of_values,
          p_search_fields,
          p_search_values,
          p_effective,
          temporal_relationships.timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;