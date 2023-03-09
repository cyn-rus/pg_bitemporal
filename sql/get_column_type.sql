CREATE OR REPLACE FUNCTION temporal_relationships.get_column_type(
	p_schema_name TEXT,
	p_table_name TEXT,                                            
	p_column_name TEXT
)
RETURNS TEXT
LANGUAGE 'sql'
COST 100.0
AS
  $function$
    SELECT t.typname :: text
    FROM pg_class c 
      JOIN pg_attribute a ON 
        c.oid = a.attrelid
        AND c.relname=p_table_name
        AND attname=p_column_name
      JOIN pg_namespace n ON
        n.oid = c.relnamespace
        AND n.nspname=p_schema_name
      JOIN pg_type t ON atttypid =t.oid;
  $function$;