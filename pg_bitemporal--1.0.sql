CREATE EXTENSION IF NOT EXISTS btree_gist;

DO $d$
  DECLARE
    domain_range_name TEXT DEFAULT 'timeperiod';
    domain_range_type TEXT DEFAULT 'tstzrange';
    domain_i_name TEXT DEFAULT 'time_endpoint';
    domain_i_type TEXT DEFAULT 'timestamptz';
  BEGIN
    -- Create timeperiod domain
    PERFORM n.nspname AS "Schema",
            t.typname AS "Name",
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS "Type"
    FROM pg_catalog.pg_type t
          LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typtype = 'd'
          AND n.nspname <> 'pg_catalog'
          AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_type_is_visible(t.oid)
      AND t.typname = domain_range_name;
      IF FOUND THEN
        RAISE NOTICE 'Domain % already exists', domain_range_name;
      ELSE
        EXECUTE FORMAT('create domain %I as %I', domain_range_name, domain_range_type);
      END IF;

    -- Create time_endpoint domain
    PERFORM n.nspname AS "Schema",
            t.typname AS "Name",
            pg_catalog.format_type(t.typbasetype, t.typtypmod) AS "Type"
    FROM pg_catalog.pg_type t
          LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
    WHERE t.typtype = 'd'
          AND n.nspname <> 'pg_catalog'
          AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_type_is_visible(t.oid)
      AND t.typname = domain_i_name;
      IF FOUND THEN
        RAISE NOTICE 'Domain % already exists', domain_i_name;
      ELSE
        EXECUTE FORMAT('create domain %I as %I', domain_i_name, domain_i_type);
      END IF;
  END;
$d$;

CREATE OR REPLACE FUNCTION timeperiod(p_range_start time_endpoint, p_range_end time_endpoint)
RETURNS timeperiod
LANGUAGE SQL IMMUTABLE AS
$func$
  SELECT tstzrange(p_range_start, p_range_end,'[)')::timeperiod;
$func$;

-- backwards compatible
CREATE OR REPLACE FUNCTION timeperiod_range(_s time_endpoint, _e time_endpoint, _ignored text)
RETURNS timeperiod
LANGUAGE SQL AS
$func$
  SELECT timeperiod(_s,_e);
$func$;

CREATE OR REPLACE FUNCTION xor(a boolean, b boolean)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS 
$$
  SELECT ((NOT a) <> (NOT b));
$$;

CREATE OR REPLACE FUNCTION fst(x anyrange)
RETURNS anyelement
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT LOWER(x);
$$;

CREATE OR REPLACE FUNCTION snd(x anyrange)
RETURNS anyelement
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT UPPER(x);
$$;

-- [starts] [starts^-1]
--
-- [starts A E]
--  A  |---|
--  E  |-------|
--
-- [starts^-1 A E]
--  A  |-------|
--  E  |---|
CREATE OR REPLACE FUNCTION has_starts(a timeperiod , b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT fst(a) = fst(b) AND snd(a) <> snd(b);
$$;

-- [finishes] [finishes^-1]
--
-- [finishes A E]
--  A  |-------|
--  E      |---|
--
-- [finishes^-1 A E]
--  A      |---|
--  E  |-------|
CREATE OR REPLACE FUNCTION has_finishes(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT snd(a) = snd(b) AND fst(a) <> fst(b);
$$;

-- [equals]
--
-- [equals A E]
--  A  |----|
--  E  |----|
CREATE OR REPLACE FUNCTION equals(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  -- doubtful = operator exists for timeperiod
 SELECT fst(a) = fst(b) AND snd(a) = snd(b);
$$;

-- [during]
--
-- [during A E]
--  A    |---|
--  E  |-------|
CREATE OR REPLACE FUNCTION is_during(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT (fst(a) > fst(b)) AND (snd(a) < snd(b));
$$;

-- [during^-1] contained
--
-- [during^-1 A E]
--  A  |-------|
--  E    |---|
CREATE OR REPLACE FUNCTION is_contained_in(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT is_during(b, a);
$$;

-- [during] or [during^-1] 
CREATE OR REPLACE FUNCTION has_during(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT is_during(a, b) OR is_during(b,a);
$$;

-- [overlaps]
--
-- [overlaps A E]
--  A  |-----|
--  E     |-----|
--
-- [overlaps^-1 A E]
--  A     |-----|
--  E  |-----|
CREATE OR REPLACE FUNCTION is_overlaps(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT fst(a) < fst(b) AND snd(a) > fst(b) AND snd(a) < snd(b);
$$;

-- either overlaps the other [overlaps] [overlaps^-1]
CREATE OR REPLACE FUNCTION has_overlaps(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT is_overlaps(a, b) OR is_overlaps(b, a) ;
$$;

-- [before]
--
-- [before A E]
--  A  |-----|
--  E           |-----|
CREATE OR REPLACE FUNCTION is_before(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT snd(a) < fst(b);
$$;

-- [before^-1]
--
-- [before^-1 A E]
--  A           |-----|
--  E   |-----|
CREATE OR REPLACE FUNCTION is_after(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  -- is_before(b, a)
  SELECT snd(b) < fst(a);
$$;

-- either [before] [before^-1]
CREATE OR REPLACE FUNCTION has_before(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT snd(a) < fst(b) OR snd(b) < fst(a);
$$;

-- [meets] [meets^-1]
--
-- no shared time tick.
--
-- [meets A E]
--  A   |-----|
--  E         |-----|
--
-- [meets^-1 A E]
--  A         |-----|
--  E   |-----|
CREATE OR REPLACE FUNCTION is_meets(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE 
AS $$
  SELECT snd(a) = fst(b) ;
$$;

CREATE OR REPLACE FUNCTION has_meets(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT snd(a) = fst(b) OR snd(b) = fst(a);
$$;

-- 
-- Partition of Allen Relationships
--

-- [Includes] 
--     [Contains] or [Overlaps]
CREATE OR REPLACE FUNCTION has_includes(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT fst(a) = fst(b) OR snd(a) = snd(b) OR
    (snd(a) <= snd(b) AND (fst(a) >= fst(b) OR fst(b) < snd(a))) OR
    (snd(a) >= snd(b) AND (fst(a) < snd(b) OR fst(a) <= fst(b)));
$$;

-- [Contains]
--    [Encloses] or [Equals]
CREATE OR REPLACE FUNCTION has_contains(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT fst(a) = fst(b) OR snd(a) = snd(b) OR
    (snd(a) < snd(b) AND fst(a) > fst(b)) OR
    (snd(b) < snd(a) AND fst(b) > fst(a));
$$;

-- [Aligns With]
--   [Starts] or [Finishes]
CREATE OR REPLACE FUNCTION has_aligns_with(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  select xor(fst(a) = fst(b), snd(a) = snd(b));
$$;

-- [Encloses]
--   [Aligns With] or [During]
CREATE OR REPLACE FUNCTION has_encloses(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT has_during(a, b) OR has_aligns_with(a, b);
$$;

-- [Excludes]
--   [Before] or [Meets]
CREATE OR REPLACE FUNCTION has_excludes(a timeperiod, b timeperiod)
RETURNS boolean
LANGUAGE SQL IMMUTABLE AS
$$
  SELECT fst(a) >= snd(b) OR fst(b) >= snd(a) ;
$$;

--
--  triggers, not null, exclusions and check 
-- all work exactly the same given the bitemporal constraints
--
-- 3 constraints do not. primary key, foreign key and unique constraints.
--

-- create the three types of constraints.
--   need strings to include in a create table
--   need commands to modify existing table

-- find the a particular set of constraints given a table
-- 

CREATE OR REPLACE FUNCTION conname_prefix()
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$f$ 
  SELECT 'bitemporal'::text;
$f$;

CREATE OR REPLACE FUNCTION mk_conname(con_type text, src_column text, fk_table text, fk_column text)
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$f$ 
  SELECT SUBSTRING(
    FORMAT('%s %s %s%s%s',
      (SELECT * FROM conname_prefix()),
      con_type,
      src_column,
      fk_table, fk_column
    )
  FROM 0 FOR 64
  );
$f$;

CREATE OR REPLACE FUNCTION mk_constraint(con_type text, con_name text, con_src text)
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$ff$ 
  SELECT FORMAT(
    $$
      CONSTRAINT %I CHECK(true OR '%s' <> '@%s@')
    $$,
    con_name,
    con_type,
    con_src
  )::text;
$ff$;

CREATE OR REPLACE FUNCTION pk_constraint(src_column text)
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$f$
  SELECT mk_constraint(
    'pk',
    mk_conname('pk', src_column, '', ''),
    src_column
  );
$f$;

CREATE OR REPLACE FUNCTION fk_constraint(src_column text, fk_table text, fk_column text, connname text)
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$ff$
  SELECT mk_constraint(
    'fk',
    connname,
    FORMAT('%s -> %s(%s)', src_column, fk_table, fk_column)
  );
$ff$;

CREATE OR REPLACE FUNCTION fk_constraint(src_column text, fk_table text, fk_column text)
RETURNS text
LANGUAGE SQL IMMUTABLE AS
$ff$ 
  SELECT fk_constraint(
    src_column,
    fk_table,
    fk_column,
    mk_conname('fk', src_column, fk_table, fk_column)
  );
$ff$;

CREATE OR REPLACE FUNCTION unique_constraint(src_column text)
RETURNS setof text
LANGUAGE SQL IMMUTABLE AS
$f$ 
  VALUES(
    mk_constraint(
      'u',
      mk_conname('u', src_column, '',''),
      FORMAT('%s', src_column)
    )
  ), (
    FORMAT(
      'CONSTRAINT %I EXCLUDE USING gist (%I WITH =, asserted WITH &&, valid WITH &&)',
      mk_conname('unique', src_column, '', ''),
      src_column
    )::text
  );
$f$;

CREATE OR REPLACE FUNCTION add_constraint(table_name text, _con text)
RETURNS text
LANGUAGE SQL IMMUTABLE
AS
$f$ 
  SELECT FORMAT('alter table %s add %s', table_name, _con)::text; 
$f$;

CREATE OR REPLACE FUNCTION select_constraint_value(src text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS
$f$ 
  DECLARE 
    at int;
    s  text;
  BEGIN
  -- select inside @ @
    at := strpos(src, '@');
    s  := substr(src, at + 1 );
    at := strpos(s, '@');
    return SUBSTRING(s from 0::int for at );
  END;
$f$;

DO $$
  BEGIN
    CREATE TYPE bitemporal_pg_constraint AS (
      oid	oid,
      conname	name,
      connamespace oid,
      contype	"char",
      condeferrable	bool,
      condeferred	bool,
      convalidated bool,
      conrelid	oid,
      contypid	oid,
      conindid	oid,
      confrelid	oid,
      confupdtype	"char",
      confdeltype	"char",
      confmatchtype	"char",
      conislocal	bool,
      coninhcount	int4,
      connoinherit	bool,
      conkey	int2[],
      confkey	int2[],
      conpfeqop	oid[],
      conppeqop	oid[],
      conffeqop	oid[],
      conexclop	oid[],
      conbin	pg_node_tree,
      consrc	text
    );
    EXCEPTION WHEN duplicate_object THEN NULL;
  END
$$;

CREATE OR REPLACE FUNCTION find_constraints(table_name text, _criteria text)
RETURNS setof bitemporal_pg_constraint
LANGUAGE SQL IMMUTABLE AS
$f$
  SELECT oid, conname, connamespace, contype,
    condeferrable, condeferred,convalidated,
    conrelid, contypid, conindid, /* conparentid,*/ confrelid,
    confupdtype, confdeltype, confmatchtype, conislocal,
    coninhcount	, connoinherit, conkey, confkey,
    conpfeqop	, conppeqop	, conffeqop	, conexclop	, conbin,
    pg_get_expr(conbin, conrelid) AS consrc -- .pg_get_constraintdef()
  FROM pg_constraint
  WHERE conrelid = cast(table_name AS regclass)
    AND conname LIKE FORMAT('%s %s %%', conname_prefix(), _criteria);
$f$;

CREATE OR REPLACE FUNCTION find_pk(table_name text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS
$f$ 
  DECLARE
    r  record;
  BEGIN
    SELECT *
    INTO r
    FROM find_constraints(table_name, 'pk');
    RETURN select_constraint_value(r.consrc);
  END;
$f$;

CREATE TABLE IF NOT EXISTS fk_constraint_type (
  conname name,
  src_column name,
  fk_table text,
  fk_column name
);

CREATE OR REPLACE FUNCTION split_out_fk(consrc text)
RETURNS fk_constraint_type
LANGUAGE plpgsql IMMUTABLE AS
$f$ 
  DECLARE
    src text;
    ref text;
    rc fk_constraint_type%ROWTYPE;
    rp int;
    lp int;
  BEGIN
    -- format('%s -> %s(%s)', src_column, fk_table, fk_column) 
    src := select_constraint_value(consrc) ;
    rc.src_column :=  split_part(src, ' ', 1);
    ref := split_part(src, ' ', 3);
    rp := strpos(ref, '(');
    lp := strpos(ref, ')');

    IF (lp < 1 or rp < 1) THEN
      RAISE NOTICE 'split_out_bitemporal_fk: invaild format "%"', consrc ;
      RETURN NULL;
    END IF;
    
    rc.fk_table := substring(ref from 0 for rp );
    rc.fk_column := substring(ref from rp +1 for (lp - rp -1) );
    RETURN rc;
  END;
$f$;

CREATE OR REPLACE FUNCTION find_fk(table_name text)
RETURNS setof fk_constraint_type
LANGUAGE plpgsql AS
$f$ 
  DECLARE
    rc fk_constraint_type%ROWTYPE;
    r record;
  BEGIN
    FOR r IN SELECT * from find_constraints(table_name, 'fk')
    LOOP
      rc := split_out_fk(r.consrc);
      rc.conname := r.conname;
      RETURN NEXT rc;
    END LOOP;
    
    RETURN;
  END;
$f$;


/*
       conname       | contype | conrelid |
consrc                                          
---------------------+---------+----------+-----------------------------------------------------------------------------------------
 bitemporal fk 1     | c       |  1625561 | (true OR ('fk'::text <> '@node_id -> sg.networks network_id@'::text))
 bitemporal fk 2     | c       |  1625561 | (true OR ('fk'::text = ANY (ARRAY['node_id'::text, 'cnu.networks'::text, 'id'::text])))
 bitemporal unique 3 | c       |  1625561 | (true OR ('col'::text = 'name'::text))

*/

-- vim: set filetype=pgsql expandtab tabstop=2 shiftwidth=2:

CREATE OR REPLACE FUNCTION create_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT,
  valid_data_type TEXT
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
      trigger_func_name TEXT;
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN
          schema_name := (SELECT split_part(p_table, '.', 1));
          temp_table_name := (SELECT split_part(p_table, '.', 2));
          v_serial_key := temp_table_name || '_key';
          v_pk_constraint_name := temp_table_name || '_pk';
          v_business_key_name := temp_table_name || '_' || translate(p_business_key, ',', '_') || '_assert_valid_excl';
          trigger_func_name := schema_name || '_' || temp_table_name || '_id_fk_validate';
      ELSE
        schema_name := 'public';
        temp_table_name := p_table;
        v_serial_key := p_table || '_key';
        v_pk_constraint_name:= p_table || '_pk';
        v_business_key_name := p_table || '_' || translate(p_business_key, ',' , '_') || '_assert_valid_excl';
        trigger_func_name := p_table || '_' || 'id_fk_validate';
      END IF;

      IF (SELECT EXISTS(
        SELECT FROM pg_tables
        WHERE schemaname = schema_name AND tablename = temp_table_name
      )) THEN
        RAISE NOTICE 'Table % has already existed', p_table;
        RETURN ('false');
      END IF;

      v_serial_key_name := v_serial_key || ' serial';

      IF (SELECT valid_data_type LIKE 'interval') THEN
        v_business_key_gist := v_business_key_gist || ' WITH =, asserted WITH &&, valid WITH &&';
        EXECUTE FORMAT(
          $create$
            CREATE TABLE %s (
              %s,
              %s,
              valid timeperiod NOT NULL,
              asserted timeperiod NOT NULL,
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
      ELSIF (SELECT valid_data_type LIKE 'event') THEN
        v_business_key_gist := v_business_key_gist || ' WITH =, asserted WITH &&';
        EXECUTE FORMAT(
          $create$
            CREATE TABLE %s (
              %s,
              %s,
              valid timestamptz NOT NULL,
              asserted timeperiod NOT NULL,
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
        RAISE EXCEPTION 'Valid data type should be either "event" or "interval"';
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

CREATE OR REPLACE FUNCTION create_interval_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT
) RETURNS boolean AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM create_bitemporal_table(
          p_table,
          p_table_definition,
          p_business_key,
          'interval'
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_event_bitemporal_table(
  p_table TEXT,
  p_table_definition TEXT,
  p_business_key TEXT
) RETURNS boolean AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM create_bitemporal_table(
          p_table,
          p_table_definition,
          p_business_key,
          'event'
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_ll_bitemporal_list_of_fields()
RETURNS boolean AS
$GBODY$
  DECLARE v_sql text;
  BEGIN
    IF current_setting('server_version_num')::int <120000 THEN 
      v_sql:= $txt$
        CREATE OR REPLACE FUNCTION ll_bitemporal_list_of_fields(p_table text)
        RETURNS text[] AS
        $BODY$
          BEGIN
            RETURN(
              array(
                SELECT attname
                FROM(
                  SELECT *
                  FROM pg_attribute
                  WHERE attrelid=p_table::regclass
                    AND attnum >0
                  ) pa LEFT OUTER JOIN pg_attrdef pad ON adrelid=p_table::regclass
                    AND adrelid=attrelid
                    AND pa.attnum=pad.adnum
                  WHERE (adsrc NOT LIKE 'nextval%' OR adsrc IS NULL)
                    AND attname !='asserted'
                    AND attname !='valid'
                    AND attname !='row_created_at'
                    AND attname NOT LIKE '%dropped%'
                  ORDER BY pa.attnum
              )
            );
          END;                        
        $BODY$
        LANGUAGE plpgsql
      $txt$;
    ELSE v_sql:= $txt$
      CREATE OR REPLACE FUNCTION ll_bitemporal_list_of_fields(p_table text)
      RETURNS text[] AS
      $BODY$
        BEGIN
          RETURN(
            array(
              SELECT attname
              FROM(
                SELECT *
                FROM pg_attribute
                WHERE attrelid=p_table::regclass
                  AND attnum >1
                ) pa LEFT OUTER JOIN pg_attrdef pad ON adrelid=p_table::regclass
                  AND adrelid=attrelid
                  AND pa.attnum=pad.adnum
                WHERE attname !='asserted'
                  AND attname !='valid'
                  AND attname !='row_created_at'
                  AND attname NOT LIKE '%dropped%'
                ORDER BY pa.attnum
            )
          );
        END;                        
      $BODY$
      LANGUAGE plpgsql
    $txt$;
    END IF;

    EXECUTE(v_sql);
    RETURN null;
  END;
$GBODY$
LANGUAGE plpgsql;

SELECT * FROM generate_ll_bitemporal_list_of_fields();
DROP FUNCTION generate_ll_bitemporal_list_of_fields();


CREATE OR REPLACE FUNCTION ll_is_bitemporal_table(p_table TEXT)
RETURNS boolean IMMUTABLE AS
$$
  DECLARE 
    v_schemaname text;
    v_tablename text;
  BEGIN  
    SELECT split_part(p_table, '.', 1) INTO v_schemaname;
    SELECT split_part(p_table, '.', 2) INTO v_tablename;
    
    RETURN(
      SELECT 
        coalesce(max(CASE WHEN a.attname='asserted' THEN 1 ELSE 0 END),0) +
          coalesce(max(CASE WHEN a.attname='valid' THEN 1 ELSE 0 END),0)=2
        AND EXISTS(
          SELECT 1
          FROM pg_attribute ac
          JOIN pg_class cc
            ON ac.attrelid=cc.oid
            AND ac.attname='row_created_at'
          JOIN pg_namespace n ON n.oid = cc.relnamespace
            AND n.nspname=v_schemaname 
            AND cc.relname=v_tablename)
        FROM pg_class c 
          JOIN pg_namespace n ON n.oid = c.relnamespace AND relkind='i'
          JOIN pg_am am ON am.oid=c.relam
          JOIN pg_index x ON c.oid=x.indexrelid
            AND amname='gist' 
            AND indisexclusion='true'
          JOIN pg_class cc ON cc.oid = x.indrelid
          JOIN pg_attribute a ON a.attrelid=c.oid
          JOIN pg_type t ON a.atttypid=t.oid
        WHERE n.nspname=v_schemaname 
          AND cc.relname=v_tablename
    );
  END;    
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ll_check_bitemporal_update_conditions(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid timeperiod  -- valid range of the update
) 
RETURNS integer AS
$BODY$
  DECLARE v_records_found integer;
  BEGIN 
    EXECUTE FORMAT(
      $s$
        SELECT count(*) 
        FROM %s
        WHERE ( %s )=( %s )
          AND (is_overlaps(valid::timeperiod, %L::timeperiod)
          OR is_meets(valid::timeperiod, %L::timeperiod)
          OR has_finishes(valid::timeperiod, %L::timeperiod))
          AND now()<@ asserted
      $s$,
      p_table,
      p_search_fields,
      p_search_values,
      p_valid,
      p_valid,
      p_valid
    ) INTO v_records_found;

    RETURN v_records_found;          
  END;    
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_correction(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_search_fields TEXT,
  p_search_values TEXT,
  p_now time_endpoint
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_list_of_fields_to_insert TEXT;
      v_table_attr TEXT[];
      v_now time_endpoint := p_now;-- for compatiability with the previous version
      v_serial_key TEXT;
      v_keys INT[];
      v_keys_old INT[];
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN v_serial_key := (SELECT split_part(p_table, '.', 2) || '_key');
      ELSE v_serial_key := p_table || '_key';
      END IF;

      v_table_attr := ll_bitemporal_list_of_fields(p_table);
      IF ARRAY_LENGTH(v_table_attr, 1) = 0
        THEN RAISE EXCEPTION 'Empty list of fields for a table: %', p_table; 
        RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');
      EXECUTE FORMAT(
        $u$
          WITH updt AS (
            UPDATE %s SET asserted = timeperiod_range(lower(asserted), %L, '[)')
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
            INSERT INTO %s ( %s, valid, asserted )
              SELECT %s, valid, timeperiod_range(upper(asserted), 'infinity', '[)')
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
 
CREATE OR REPLACE FUNCTION bitemporal_correction(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_search_fields TEXT,
  p_search_values TEXT
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_correction(
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

CREATE OR REPLACE FUNCTION bitemporal_delete(
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
          UPDATE %s SET asserted = timeperiod(LOWER(asserted), %L::timestamptz)
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

CREATE OR REPLACE FUNCTION bitemporal_delete(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT  --  search values
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_delete(
          p_table,
          p_search_fields,
          p_search_values,
          now()
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_inactivate(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT, --  search values
  p_valid timeperiod, -- inactive starting
  p_asserted timeperiod -- will be asserted
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_list_of_fields_to_insert TEXT := ' ';
      v_list_of_fields_to_insert_excl_valid TEXT;
      v_table_attr TEXT[];
      v_now timestamptz := now(); -- so that we can reference this time
      v_keys INT[];
      v_keys_old INT[];
      v_serial_key TEXT;
    BEGIN 
    IF LOWER(p_asserted) < v_now::date --should we allow this precision?...
      OR UPPER(p_asserted) < 'infinity'
      THEN RAISE EXCEPTION 'Asserted interval starts in the past or has a finite end: %', p_asserted; 
        RETURN v_rowcount;
    END IF;

    IF (SELECT p_table LIKE '%.%')
      THEN v_serial_key := (SELECT split_part(p_table, '.', 2) || '_key');
    ELSE v_serial_key := p_table || '_key';
    END IF;
    
    v_table_attr := ll_bitemporal_list_of_fields(p_table);
    IF ARRAY_LENGTH(v_table_attr, 1) = 0
      THEN RAISE EXCEPTION 'Empty list of fields for a table: %', p_table; 
        RETURN v_rowcount;
    END IF;
    v_list_of_fields_to_insert_excl_valid := ARRAY_TO_STRING(v_table_attr, ',', '');
    v_list_of_fields_to_insert := v_list_of_fields_to_insert_excl_valid || ',valid';
    
  --end assertion period for the old record(s)

    EXECUTE FORMAT(
      $u$
        WITH updt AS (
          UPDATE %s 
          SET asserted = timeperiod(LOWER(asserted), lower(%L::timeperiod))
          WHERE ( %s )=( %s ) AND (is_overlaps(valid, %L)
              OR is_meets(valid, %L)
              OR has_finishes(valid, %L))
            AND (is_overlaps(asserted, %L) --now() <@ asserted
              OR has_finishes(asserted, %L)) RETURNING %s)
        SELECT array_agg(%s) FROM updt
      $u$,  
      p_table,
      p_asserted,
      p_search_fields,
      p_search_values,
      p_valid,
      p_valid,
      p_valid,
      p_asserted,
      p_asserted,
      v_serial_key,
      v_serial_key
    ) INTO v_keys_old;
            
    --insert new assertion range with old values and valid-ended
    EXECUTE FORMAT(
      $i$
        INSERT INTO %s (%s, valid, asserted)
        SELECT %s, timeperiod(LOWER(valid), LOWER(%L::timeperiod)), %L
        FROM %s
        WHERE ( %s ) in ( %s )
      $i$,
      p_table,
      v_list_of_fields_to_insert_excl_valid,
      v_list_of_fields_to_insert_excl_valid,
      p_valid,
      p_asserted,
      p_table,
      v_serial_key,
      COALESCE(array_to_string(v_keys_old,','),'NULL')
    );

    GET DIAGNOSTICS v_rowcount:=ROW_COUNT; 
    RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_inactivate(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid timeperiod -- inactive starting
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_inactivate(
          p_table,
          p_search_fields,
          p_search_values,
          p_valid,
          timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_inactivate(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT  --  search values
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_inactivate(
          p_table,
          p_search_fields,
          p_search_values,
          timeperiod(now(), 'infinity'),
          timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_valid anyelement,
  p_asserted timeperiod
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
    BEGIN
      IF (SELECT * FROM bitemporal_is_data_type_correct(p_table, p_valid))
        THEN EXECUTE FORMAT(
          $i$
            INSERT INTO %s (%s, valid, asserted)
            VALUES (%s, %L, %L)
            RETURNING *
          $i$,
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_valid,
          p_asserted
        );
        GET DIAGNOSTICS v_rowcount := ROW_COUNT;
      END IF;

      RETURN v_rowcount;
    END;    
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT,
  p_valid anyelement
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      RETURN (
        SELECT * FROM bitemporal_insert(
          p_table,
          p_list_of_fields,
          p_list_of_values,
          p_valid,
          timeperiod(now(), 'infinity')
        )
      );
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_insert(
  p_table TEXT,
  p_list_of_fields TEXT,
  p_list_of_values TEXT
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      table_type TEXT;
    BEGIN
      table_type := (SELECT * FROM bitemporal_table_type(p_table));
      IF table_type = 'interval' THEN
        RETURN (
          SELECT * FROM bitemporal_insert(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            timeperiod(now(), 'infinity'),
            timeperiod(now(), 'infinity')
          )
        );
      ELSE 
        RETURN (
          SELECT * FROM bitemporal_insert(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            now(),
            timeperiod(now(), 'infinity')
          )
        );
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_update(
  p_table TEXT,
  p_list_of_fields text, -- fields to update
  p_list_of_values TEXT,  -- values to update with
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid anyelement,  -- valid range of the update
  p_asserted timeperiod  -- assertion for the update
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_list_of_fields_to_insert TEXT := ' ';
      v_table_attr TEXT[];
      v_serial_key TEXT;
      v_keys_old INT[];
      v_keys INT[];
      v_now timestamptz := now(); -- so that we can reference this time
    BEGIN
      IF NOT (SELECT * FROM bitemporal_is_data_type_correct(p_table, p_valid))
        THEN RETURN v_rowcount;
      END IF;

      -- IF LOWER(p_asserted) < v_now::date -- should we allow this precision?...
      --   OR UPPER(p_asserted) < 'infinity'
      --     THEN RAISE NOTICE 'Asserted interval starts in the future or has a finite end: %', p_asserted;
      --     RETURN v_rowcount;
      -- END IF;

      IF (SELECT p_table LIKE '%.%')
        THEN v_serial_key := (SELECT split_part(p_table, '.', 2)) || '_key';
      ELSE v_serial_key := p_table || '_key';
      END IF;

      v_table_attr := ll_bitemporal_list_of_fields(p_table);
      IF array_length(v_table_attr, 1) = 0
        THEN RAISE NOTICE 'Empty list of fields for a table %', p_table;
        RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');

      v_keys_old := (SELECT * FROM bitemporal_update_end_assertion(
        p_table,
        p_search_fields,
        p_search_values,
        p_valid,
        p_asserted,
        v_serial_key,
        v_list_of_fields_to_insert
      ));

      -- Insert new assertion with old values and new valid
      EXECUTE FORMAT(
        $i$
          WITH inst AS (
            INSERT INTO %s ( %s, valid, asserted )
              SELECT %s ,%L, %L
              FROM %s
              WHERE ( %s ) IN (%s )
            RETURNING %s
          )
          SELECT array_agg(%s)
          FROM inst
        $i$,
        p_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        p_valid,
        p_asserted,
        p_table,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys_old,','), 'NULL'),
        v_serial_key,
        v_serial_key
      ) INTO v_keys;

      -- Update new record(s) in new assertion rage with new values                                  
      EXECUTE FORMAT(
        $u$
          UPDATE %s SET (%s) =
            (SELECT %s) 
            WHERE ( %s ) IN ( %s )
        $u$,
        p_table,
        p_list_of_fields,
        p_list_of_values,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys,','), 'NULL')
      );

      GET DIAGNOSTICS v_rowcount := ROW_COUNT;  
      RETURN v_rowcount;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_update_end_assertion(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid timeperiod,
  p_asserted timeperiod,  -- assertion for the update
  v_serial_key TEXT,
  v_list_of_fields_to_insert TEXT
) RETURNS INTEGER[] AS
  $BODY$
    DECLARE
      v_keys_old INT[];
    BEGIN
      -- End assertion period for the old record(s)
      EXECUTE FORMAT(
        $u$
          WITH updt AS (
            UPDATE %s SET asserted = timeperiod(LOWER(asserted), LOWER(%L::timeperiod))
            WHERE ( %s )=( %s ) AND
              (is_overlaps(valid, %L)
                OR is_meets(valid::timeperiod, %L)
                OR has_finishes(valid::timeperiod, %L))
              AND now() <@ asserted
            RETURNING %s
          )
          SELECT array_agg(%s)
          FROM updt
        $u$,
        p_table,
        p_asserted,
        p_search_fields,
        p_search_values,
        p_valid,
        p_valid,
        p_valid,
        v_serial_key,
        v_serial_key
      ) INTO v_keys_old;

      -- Insert new assertion rage with old values and valid-ended
      EXECUTE FORMAT(
        $i$
          INSERT INTO %s ( %s, valid, asserted )
            SELECT %s, timeperiod(LOWER(valid), LOWER(%L::timeperiod)), %L
            FROM %s
            WHERE ( %s ) in ( %s )
        $i$,
        p_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        p_valid,
        p_asserted,
        p_table,
        v_serial_key,
        COALESCE(ARRAY_TO_STRING(v_keys_old,','), 'NULL')
      );
      
      RETURN v_keys_old;
    END;    
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_update_end_assertion(
  p_table TEXT,
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid timestamptz,
  p_asserted timeperiod,  -- assertion for the update
  v_serial_key TEXT,
  v_list_of_fields_to_insert TEXT
) RETURNS INTEGER[] AS
  $BODY$
    DECLARE
      v_keys_old INT[];
    BEGIN
      -- End assertion period for the old record(s)
      EXECUTE FORMAT(
        $u$
          WITH updt AS (
            UPDATE %s SET asserted = timeperiod(LOWER(asserted), LOWER(%L::timeperiod))
            WHERE ( %s )=( %s )
            RETURNING %s
          )
          SELECT array_agg(%s)
          FROM updt
        $u$,
        p_table,
        p_asserted,
        p_search_fields,
        p_search_values,
        v_serial_key,
        v_serial_key
      ) INTO v_keys_old;

      RETURN v_keys_old;
    END;    
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_update(
  p_table TEXT,
  p_list_of_fields text, -- fields to update
  p_list_of_values TEXT,  -- values to update with
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT,  --  search values
  p_valid anyelement  -- valid range of the update
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      IF (SELECT * FROM bitemporal_is_data_type_correct(p_table, p_valid))
        THEN RETURN (
          SELECT * FROM bitemporal_update(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            p_search_fields,
            p_search_values,
            p_valid,
            timeperiod(now(), 'infinity')
          )
        );
      ELSE RETURN 0;
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_update(
  p_table TEXT,
  p_list_of_fields text, -- fields to update
  p_list_of_values TEXT,  -- values to update with
  p_search_fields TEXT,  -- search fields
  p_search_values TEXT  --  search values
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      table_type TEXT;
    BEGIN
      table_type := (SELECT * FROM bitemporal_table_type(p_table));
      IF table_type = 'interval' THEN
        RETURN (
          SELECT * FROM bitemporal_update(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            p_search_fields,
            p_search_values,
            timeperiod(now(), 'infinity'),
            timeperiod(now(), 'infinity')
          )
        );
      ELSE 
        RETURN (
          SELECT * FROM bitemporal_update(
            p_table,
            p_list_of_fields,
            p_list_of_values,
            p_search_fields,
            p_search_values,
            now(),
            timeperiod(now(), 'infinity')
          )
        );
      END IF;
   END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_table_type(
  p_table TEXT
) RETURNS TEXT AS
  $BODY$
    DECLARE
      query TEXT;
      data_type TEXT;
      schema_name TEXT;
      temp_table_name TEXT;
    BEGIN
      IF (SELECT p_table LIKE '%.%')
        THEN
          schema_name := (SELECT split_part(p_table, '.', 1));
          temp_table_name := (SELECT split_part(p_table, '.', 2));
      ELSE
        schema_name := 'public';
        temp_table_name := p_table;
      END IF;

      query := FORMAT(
        $i$
          SELECT data_type
          FROM information_schema.columns
          WHERE table_name = '%s'
            AND table_schema = '%s'
            AND column_name = 'valid'
        $i$,
        temp_table_name,
        schema_name
      );
      EXECUTE query INTO data_type;

      IF (SELECT data_type = 'tstzrange')
        THEN RETURN('interval');
      ELSIF (SELECT data_type = 'timestamp with time zone')
        THEN RETURN('event'); 
      ELSE
        RAISE EXCEPTION 'Invalid data type for valid';
        RETURN('unknown'); 
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_is_data_type_correct(
  p_table TEXT,
  p_valid anyelement
) RETURNS BOOLEAN AS
  $BODY$
    DECLARE
      table_type TEXT := (SELECT * FROM bitemporal_table_type(p_table));
      valid_type TEXT := (SELECT pg_typeof(p_valid));
      is_correct BOOLEAN := 'false';
    BEGIN
      IF (valid_type = 'timeperiod' AND table_type = 'interval')
        OR (valid_type = 'timestamp with time zone' AND table_type = 'event')
          THEN is_correct = 'true';
      ELSIF valid_type != 'timeperiod' OR valid_type != 'timestamp with time zone'
        THEN RAISE NOTICE 'Valid type is incorrect'
          USING HINT = 'Valid must be timeperiod or timestamptz';
      ELSE
        RAISE NOTICE 'Valid type does not match with the table';
      END IF;

      RETURN is_correct;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_correction_valid(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT,
  p_valid anyelement,
  p_asserted time_endpoint
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      v_rowcount INT := 0;
      v_table_attr TEXT[];
      v_list_of_fields_to_insert TEXT := ' ';
      tes TEXT;
    BEGIN
      IF NOT (SELECT * FROM bitemporal_is_data_type_correct(p_table, p_valid))
        THEN RETURN v_rowcount;
      END IF;

      v_table_attr := ll_bitemporal_list_of_fields(p_table);
      IF array_length(v_table_attr, 1) = 0
        THEN RAISE NOTICE 'Empty list of fields for a table %', p_table;
        RETURN v_rowcount;
      END IF;

      v_rowcount := (SELECT * FROM bitemporal_delete(p_table, p_search_field, p_search_value, p_asserted));
      
      IF (v_rowcount = 0)
        THEN RETURN v_rowcount;
      END IF;

      v_list_of_fields_to_insert := ARRAY_TO_STRING(v_table_attr, ',', '');

      EXECUTE FORMAT(
        $i$
          INSERT INTO %s ( %s, valid, asserted )
            SELECT %s, %L, timeperiod_range(%L, 'infinity', '[)]')
            FROM %s
            WHERE %s = %s
            ORDER BY row_created_at DESC
            LIMIT 1
        $i$,
        p_table,
        v_list_of_fields_to_insert,
        v_list_of_fields_to_insert,
        p_valid,
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

CREATE OR REPLACE FUNCTION bitemporal_correction_valid(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT,
  p_valid anyelement
) RETURNS INTEGER AS
  $BODY$
    BEGIN
      IF (SELECT * FROM bitemporal_is_data_type_correct(p_table, p_valid))
        THEN RETURN (
          SELECT * FROM bitemporal_correction_valid(
            p_table,
            p_search_field,
            p_search_value,
            p_valid,
            now()
          )
        );
      ELSE RETURN 0;
      END IF;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_correction_valid(
  p_table TEXT,
  p_search_field TEXT,
  p_search_value TEXT
) RETURNS INTEGER AS
  $BODY$
    DECLARE
      table_type TEXT;
    BEGIN
      table_type := (SELECT * FROM bitemporal_table_type(p_table));
      
      IF table_type = 'interval' THEN
        RETURN (
          SELECT * FROM bitemporal_correction_valid(
            p_table,
            p_search_field,
            p_search_value,
            timeperiod(now(), 'infinity'),
            now()
          )
        );
      ELSE
        RETURN (
          SELECT * FROM bitemporal_correction_valid(
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

CREATE OR REPLACE PROCEDURE bitemporal_fk_constraint(
  parent_table TEXT,
  parent_business_key TEXT,
  child_table TEXT,
  child_business_key TEXT
) AS
  $BODY$
    DECLARE
      trigger_func_name TEXT;
      trigger_name TEXT;
      p_temp_table_name TEXT;
      p_temp_schema_name TEXT;
      c_temp_table_name TEXT;
      c_temp_schema_name TEXT;
      p_valid_type TEXT := (SELECT * FROM bitemporal_table_type(parent_table));
      c_valid_type TEXT := (SELECT * FROM bitemporal_table_type(child_table));
      is_parent_bitemporal BOOLEAN := (SELECT p_valid_type LIKE 'unknown');
      is_child_bitemporal BOOLEAN := (SELECT c_valid_type LIKE 'unknown');
    BEGIN
      IF (is_parent_bitemporal IS TRUE) OR (is_child_bitemporal IS TRUE) THEN
        RAISE NOTICE 'Table is not bitemporal';
        RETURN;
      END IF;

      IF (SELECT parent_table LIKE '%.%') THEN
        p_temp_schema_name := (SELECT split_part(parent_table, '.', 1));
        p_temp_table_name := (SELECT split_part(parent_table, '.', 2));
      ELSE
        p_temp_schema_name := 'public';
        p_temp_table_name := parent_table;
      END IF;

      IF (SELECT child_table LIKE '%.%') THEN
        c_temp_schema_name := (SELECT split_part(child_table, '.', 1));
        c_temp_table_name := (SELECT split_part(child_table, '.', 2));
        trigger_name := c_temp_schema_name || '_' || c_temp_table_name || '_' || child_business_key || '_fk';
        trigger_func_name := 
          p_temp_schema_name || '_' ||
          p_temp_table_name || '_' ||
          c_temp_schema_name || '_' ||
          c_temp_table_name ||
          '_fk_validate';
      ELSE
        c_temp_schema_name := 'public';
        c_temp_table_name := child_table;
        trigger_name := child_table || '_' || child_business_key || '_fk';
        trigger_func_name :=
          parent_table || '_' ||
          child_table || '_' ||
          '_fk_validate';
      END IF;

      IF (p_valid_type = 'interval') THEN
        EXECUTE FORMAT(
          $trigger$
            CREATE OR REPLACE FUNCTION %s()
            RETURNS trigger AS
            $b$
              DECLARE
                v_value integer;
                v_result boolean;
                v_cnt int;
              BEGIN
                v_value := NEW.%s;
                IF v_value IS NOT NULL THEN
                  SELECT COUNT(*) INTO v_cnt
                  FROM %s a
                  WHERE %s = v_value AND NEW.valid <@ a.valid AND NEW.asserted <@ a.asserted;

                  IF v_cnt = 0 THEN
                    RAISE EXCEPTION 'Foreign key constraint validated';
                  END IF;
                END IF;

                RETURN NEW;
              END;
            $b$
            LANGUAGE plpgsql;
          $trigger$,
          trigger_func_name,
          child_business_key,
          parent_table,
          parent_business_key 
        );
      ELSE
        EXECUTE FORMAT(
          $trigger$
            CREATE OR REPLACE FUNCTION %s()
            RETURNS trigger AS
            $b$
              DECLARE
                v_value integer;
                v_result boolean;
                v_cnt int;
              BEGIN
                v_value := NEW.%s;
                IF v_value IS NOT NULL THEN
                  SELECT COUNT(*) INTO v_cnt
                  FROM %s a
                  WHERE %s = v_value AND NEW.asserted <@ a.asserted;

                  IF v_cnt = 0 THEN
                    RAISE EXCEPTION 'Foreign key constraint validated';
                  END IF;
                END IF;

                RETURN NEW;
              END;
            $b$
            LANGUAGE plpgsql;
          $trigger$,
          trigger_func_name,
          child_business_key,
          parent_table,
          parent_business_key 
        );
      END IF;

      EXECUTE FORMAT(
        $t$
          CREATE TRIGGER %s
          BEFORE INSERT OR UPDATE ON %s
          FOR EACH ROW
          EXECUTE PROCEDURE %s();
        $t$,
        trigger_name,
        child_table,
        trigger_func_name
      ); 
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bitemporal_coalesce(
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

      table_type := (SELECT * FROM bitemporal_table_type(p_table));
      table_columns_array := ll_bitemporal_list_of_fields(p_table);
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
