CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_effective anyelement,
  p_asserted temporal_relationships.timeperiod
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
    BEGIN
      IF (SELECT * FROM bitemporal_internal.ll_is_data_type_correct(p_table, p_effective))
        THEN EXECUTE FORMAT(
          $i$
            INSERT INTO %s (%s, effective, asserted)
            VALUES (%s, %L, %L)
            RETURNING *
          $i$,
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_effective,
          p_asserted
        );
        GET DIAGNOSTICS v_rowcount := ROW_COUNT;
      END IF;

      RETURN v_rowcount;
    END;    
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_effective anyelement
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_effective,
          temporal_relationships.timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      table_type TEXT;
    BEGIN
      table_type := (SELECT * FROM bitemporal_internal.ll_bitemporal_table_type(p_table));
      IF table_type = 'interval' THEN
        RETURN (
          SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            temporal_relationships.timeperiod(now(), 'infinity'),
            temporal_relationships.timeperiod(now(), 'infinity')
          )
        );
      ELSE 
        RETURN (
          SELECT * FROM bitemporal_internal.ll_bitemporal_insert(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            now(),
            temporal_relationships.timeperiod(now(), 'infinity')
          )
        );
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;