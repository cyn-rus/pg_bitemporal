CREATE OR REPLACE FUNCTION bitemporal_internal.ll_is_data_type_correct(
  p_table TEXT,
  p_effective anyelement
) RETURNS BOOLEAN AS
  $BODY$
    DECLARE
      table_type TEXT := (SELECT * FROM bitemporal_internal.ll_bitemporal_table_type(p_table));
      effective_type TEXT := (SELECT pg_typeof(p_effective));
    BEGIN
      IF (effective_type = 'temporal_relationships.timeperiod' AND table_type = 'period')
        OR (effective_type = 'timestamp with time zone' AND table_type = 'event')
          THEN RETURN ('true');
      ELSIF effective_type != 'temporal_relationships.timeperiod' OR effective_type != 'timestamp with time zone'
        THEN RAISE EXCEPTION 'Effective type is incorrect'
          USING HINT = 'Type must be temporal_relationships.timeperiod or timestamptz';
      ELSE
        RAISE EXCEPTION 'Effective type does not match with the table';
      END IF;

      RETURN ('false');
    END;
  $BODY$
LANGUAGE plpgsql;