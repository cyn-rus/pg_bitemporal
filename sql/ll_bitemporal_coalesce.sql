CREATE OR REPLACE FUNCTION bitemporal_internal.ll_bitemporal_coalesce(
  p_table TEXT
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      table_type TEXT;
      v_serial_key TEXT;
      table_columns_array TEXT[];
      table_columns_string TEXT;
      view_name TEXT := p_table || '_view';

      upper_asserted TEXT = '';
      lower_asserted TEXT = '';
      is_coalesce BOOLEAN := false;
      loop_result RECORD;
      loop_result_text TEXT;
      temp TEXT;

      curr_record TEXT := '';
      prev_record TEXT := '';
      count INT;
      curr_key TEXT;
      prev_key TEXT;

      coalesce_keys TEXT[] DEFAULT '{}';

      asserted_start TEXT;
      asserted_end TEXT;

      valid_value TEXT;
      valid_start TEXT;
      valid_end TEXT;
    BEGIN
      IF (SELECT p_table LIKE '%.%') THEN
        v_serial_key := (SELECT split_part(p_table, '.', 2)) || '_key';
      ELSE
        v_serial_key := p_table || '_key';
      END IF;

      table_type := (SELECT * FROM bitemporal_internal.ll_bitemporal_table_type(p_table));
      table_columns_array := bitemporal_internal.ll_bitemporal_list_of_fields(p_table);
      IF ARRAY_LENGTH(table_columns_array, 1) = 0 THEN
        RAISE NOTICE 'Empty list of fields for a table %', p_table;
        RETURN v_rowcount;
      END IF;

      table_columns_string := array_to_string(table_columns_array, ', ');

      EXECUTE FORMAT(
        $o$
          CREATE OR REPLACE VIEW %s AS
            SELECT *
            FROM %s
            ORDER BY %s, row_created_at
        $o$,
        view_name,
        p_table,
        table_columns_string
      );

      FOR loop_result IN
      EXECUTE FORMAT(
        $for$
          SELECT *
          FROM %s
        $for$,
        view_name
      )
      LOOP
        loop_result_text := TEXT(ARRAY[loop_result]);
        curr_key := split_part(split_part(loop_result_text, ',', 1), '(', 2);
        temp := LTRIM(loop_result_text, split_part(loop_result_text, ',', 1));

        IF (table_type = 'interval') THEN
          curr_record := REPLACE(split_part(temp, split_part(temp, ',\"[\"\"', 3), 1), '\"', '''');
          curr_record := REPLACE(curr_record, '''''', '''');
          curr_record := LEFT(SUBSTRING(curr_record, 2, length(curr_record)), -4);
        ELSE
          curr_record := RIGHT(REPLACE(split_part(temp, ',\"[\"\"', 1), '\"', ''''), -1);
        END IF;

        lower_asserted := LOWER(loop_result.asserted);

        IF upper_asserted = lower_asserted THEN
          IF curr_record = prev_record THEN
            is_coalesce := true;
            asserted_end := UPPER(loop_result.asserted);

            coalesce_keys := array_append(coalesce_keys, prev_key);
            coalesce_keys := array_append(coalesce_keys, curr_key);
            coalesce_keys := ARRAY(SELECT DISTINCT * FROM unnest(coalesce_keys));
          END IF;
        ELSIF lower_asserted != '' THEN
          IF is_coalesce THEN
            is_coalesce := false;
            valid_value := split_part(prev_record, '[', 2);
            valid_start := split_part(valid_value, ',', 1);
            valid_end := LEFT(split_part(valid_value, ',', 2), -2);
            prev_record := split_part(prev_record, ',''[' ,1);
            prev_record := REPLACE(prev_record, '''', '');
            prev_record := '''' || REPLACE(prev_record, ',', ''',''') || '''';

            FOR count IN 1..array_length(coalesce_keys, 1) LOOP
              EXECUTE FORMAT(
                $del$
                  DELETE FROM %s
                  WHERE %s = %s
                $del$,
                p_table,
                v_serial_key,
                coalesce_keys[count]
              );
            END LOOP;

            IF (table_type = 'interval') THEN
              valid_start := split_part(valid_value, ',', 1);
              valid_end := LEFT(split_part(valid_value, ',', 2), -2);
              EXECUTE FORMAT(
                $insert$
                  INSERT INTO %s(%s, valid, asserted)
                  VALUES(%s, timeperiod('%s', '%s'), timeperiod('%s', '%s'))
                $insert$,
                p_table,
                table_columns_string,
                prev_record,
                REPLACE(valid_start, '''', ''),
                REPLACE(valid_end, '''', ''),
                REPLACE(asserted_start, '''', ''),
                REPLACE(asserted_end, '''', '')
              );
            ELSE
              prev_record := prev_record || '::timestamptz';
              EXECUTE FORMAT(
                $insert$
                  INSERT INTO %s(%s, valid, asserted)
                  VALUES(%s, timeperiod('%s', '%s'))
                $insert$,
                p_table,
                table_columns_string,
                prev_record,
                REPLACE(asserted_start, '''', ''),
                REPLACE(asserted_end, '''', '')
              );
            END IF;
          END IF;
        END IF;

        IF NOT is_coalesce THEN
          asserted_start := lower_asserted;
        END IF;

        upper_asserted := UPPER(loop_result.asserted);
        prev_record := curr_record;
        prev_key := curr_key;
      END LOOP;

      IF is_coalesce THEN
        prev_record := split_part(prev_record, ',''[' ,1);
        prev_record := REPLACE(prev_record, '''', '');
        prev_record := '''' || REPLACE(prev_record, ',', ''',''') || '''';

        FOR count IN 1..array_length(coalesce_keys, 1) LOOP
          EXECUTE FORMAT(
            $del$
              DELETE FROM %s
              WHERE %s = %s
            $del$,
            p_table,
            v_serial_key,
            coalesce_keys[count]
          );
        END LOOP;

        IF (table_type = 'interval') THEN
          valid_value := split_part(prev_record, '[', 2);
          valid_start := split_part(valid_value, ',', 1);
          valid_end := LEFT(split_part(valid_value, ',', 2), -2);
          EXECUTE FORMAT(
            $insert$
              INSERT INTO %s(%s, valid, asserted)
              VALUES(%s, timeperiod('%s', '%s'), timeperiod('%s', '%s'))
            $insert$,
            p_table,
            table_columns_string,
            prev_record,
            REPLACE(valid_start, '''', ''),
            REPLACE(valid_end, '''', ''),
            REPLACE(asserted_start, '''', ''),
            REPLACE(asserted_end, '''', '')
          );
        ELSE
          prev_record := prev_record || '::timestamptz';
          EXECUTE FORMAT(
            $insert$
              INSERT INTO %s(%s, valid, asserted)
              VALUES(%s, timeperiod('%s', '%s'))
            $insert$,
            p_table,
            table_columns_string,
            prev_record,
            REPLACE(asserted_start, '''', ''),
            REPLACE(asserted_end, '''', '')
          );
        END IF;
      END IF;
      GET DIAGNOSTICS v_rowcount := ROW_COUNT;  
      RETURN v_rowcount;
   END;
  $BODY$
LANGUAGE plpgsql;
