CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_delete(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_asserted timestamptz -- will be asserted
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
    BEGIN 
  --end assertion period for the current records record(s)

      EXECUTE format(
        $u$
          UPDATE %s SET asserted = temporal_relationships.timeperiod(LOWER(asserted), %L::timestamptz)
          WHERE ( %s ) = ( %s )
            AND %L::timestamptz <@ asserted
        $u$,
        p_table,
        p_asserted,
        p_search_fields,
        p_search_values,
        p_asserted
      );

      GET DIAGNOSTICS v_rowcount := ROW_COUNT; 
      RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_delete(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT  --  search values
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_bitemporal_delete(
          p_table,
          p_search_fields,
          p_search_values,
          now()
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;