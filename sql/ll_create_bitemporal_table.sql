CREATE OR REPLACE FUNCTION bitemporal_internal.ll_create_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT,
  effective_data_type TEXT
) RETURNS boolean AS
  $BODY$
    DECLARE
      v_business_key_name TEXT;
      v_business_key_gist TEXT := replace(p_business_key, ',' ,' WITH =,');
      v_serial_key_name TEXT;
      v_serial_key TEXT;
      v_pk_constraint_name TEXT;
      v_table_definition TEXT := replace(p_table_definition, ' serial', ' integer');
      v_error TEXT;
      v_business_key_array TEXT[] := string_to_array(p_business_key, ',');
      i INT;
      temp_table_name TEXT;
      schema_name TEXT;
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN
          schema_name := (SELECT split_part(p_table, '.', 1));
          temp_table_name := (SELECT split_part(p_table, '.', 2));
          v_serial_key := temp_table_name || '_key';
          v_pk_constraint_name := temp_table_name || '_pk';
          v_business_key_name := temp_table_name || '_' || translate(p_business_key, ',', '_') || '_assert_eff_excl';
      ELSE
        schema_name := 'public';
        temp_table_name := p_table;
        v_serial_key := p_table || '_key';
        v_pk_constraint_name:= p_table || '_pk';
        v_business_key_name := p_table || '_' || translate(p_business_key, ',' , '_') || '_assert_eff_excl';
      END IF;

      IF (SELECT EXISTS(
        SELECT FROM pg_tables
        WHERE schemaname = schema_name AND tablename = temp_table_name
      )) THEN
        RAISE NOTICE 'Table % has already existed', p_table;
        RETURN ('false');
      END IF;

      v_serial_key_name := v_serial_key || ' serial';

      IF (SELECT effective_data_type LIKE 'interval') THEN
        v_business_key_gist := v_business_key_gist || ' WITH =, asserted WITH &&, effective WITH &&';
        EXECUTE FORMAT(
          $create$
            CREATE TABLE %s (
              %s,
              %s,
              effective temporal_relationships.timeperiod NOT NULL,
              asserted temporal_relationships.timeperiod NOT NULL,
              row_created_at timestamptz NOT NULL DEFAULT now(),
              CONSTRAINT %s PRIMARY KEY (%s),
              CONSTRAINT %s EXCLUDE USING gist (%s)
            )
          $create$,
          p_table,
          v_serial_key_name,
          v_table_definition,
          v_pk_constraint_name,
          v_serial_key,
          v_business_key_name,
          v_business_key_gist
        );
      ELSIF (SELECT effective_data_type LIKE 'event') THEN
        v_business_key_gist := v_business_key_gist || ' WITH =, asserted WITH &&';
        EXECUTE FORMAT(
          $create$
            CREATE TABLE %s (
              %s,
              %s,
              effective timestamptz NOT NULL,
              asserted temporal_relationships.timeperiod NOT NULL,
              row_created_at timestamptz NOT NULL DEFAULT now(),
              CONSTRAINT %s PRIMARY KEY (%s),
              CONSTRAINT %s EXCLUDE USING gist (%s)
            )
          $create$,
          p_table,
          v_serial_key_name,
          v_table_definition,
          v_pk_constraint_name,
          v_serial_key,
          v_business_key_name,
          v_business_key_gist
        );
      ELSE
        RAISE EXCEPTION 'Effective data type should be either "event" or "period"';
        RETURN('false');
      END IF;

      i := 1;
      WHILE v_business_key_array[i] IS NOT NULL LOOP
        EXECUTE FORMAT(
          $alter$
            ALTER TABLE %s ALTER %s SET NOT NULL
          $alter$,
          p_table,
          v_business_key_array[i]
        );
        i := i+1;
      END LOOP;
      
      RETURN ('true');
      EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error = MESSAGE_TEXT;
        RAISE NOTICE '%', v_error;
        RETURN ('false');
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_create_interval_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT
) RETURNS boolean AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
          p_table,
          p_table_definition,
          p_business_key,
          'interval'
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_internal.ll_create_event_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT
) RETURNS boolean AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_internal.ll_create_bitemporal_table(
          p_table,
          p_table_definition,
          p_business_key,
          'event'
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;