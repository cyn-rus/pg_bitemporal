CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction_effective(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT,
  p_effective anyelement,
  p_asserted temporal_relationships.time_endpoint
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_table_attr TEXT[];
      v_list_of_fields_to_insert TEXT := ' ';
      tes TEXT;
    BEGIN
      IF NOT (SELECT * FROM bitemporal_internal.ll_is_data_type_correct(p_table, p_effective))
        THEN RETURN v_rowcount;
      END IF;

      v_table_attr := bitemporal_internal.ll_bitemporal_list_of_fields(p_table);
      IF array_length(v_table_attr, 1) = 0
        THEN RAISE NOTICE 'Empty list of fields for a table %', p_table;
        RETURN v_rowcount;
      END IF;

      v_rowcount := (SELECT * FROM bitemporal_internal.ll_bitemporal_delete(p_table, p_search_field, p_search_value, p_asserted));
      
      IF (v_rowcount = 0)
        THEN RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');

      EXECUTE FORMAT(
        $i$
          INSERT INTO %s ( %s, effective, asserted )
            SELECT %s, %L, temporal_relationships.timeperiod_range(%L, 'infinity', '[)]')
            FROM %s
            WHERE %s = %s
            ORDER BY row_created_at DESC
            LIMIT 1
        $i$,
        p_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        p_effective,
        p_asserted,
        p_table,
        p_search_field,
        p_search_value
      );

      GET DIAGNOSTICS v_rowcount := ROW_COUNT;
      RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction_effective(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT,
  p_effective timestamptz
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_correction_effective(
          p_table,
          p_search_field,
          p_search_value,
          p_effective,
          now()
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_correction_effective(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      table_type TEXT;
    BEGIN
      table_type := (SELECT * FROM bitemporal_internal.ll_bitemporal_table_type(p_table));
      
      IF table_type = 'interval' THEN
        RETURN (
          SELECT * FROM bitemporal_internal.ll_bitemporal_correction_effective(
            p_table,
            p_search_field,
            p_search_value,
            temporal_relationships.timeperiod(now(), 'infinity'),
            now()
          )
        );
      ELSE
        RETURN (
          SELECT * FROM bitemporal_internal.ll_bitemporal_correction_effective(
            p_table,
            p_search_field,
            p_search_value,
            now(),
            now()
          )
        );
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;