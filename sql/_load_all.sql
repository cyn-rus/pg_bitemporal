\echo Start load all bitemporal code
\set ON_ERROR_STOP on
\pset pager off
-- toggle timing to get a better idea of what is going on
--\timing on
set client_min_messages to warning;

\ir extensions.sql
\ir relationships.sql

begin;
set search_path to bitemporal_internal, public;


\ir bitemporal_internal_schema.sql

\ir  metadata.sql

\ir ll_create_bitemporal_table.sql
\ir ll_bitemporal_list_of_fields.sql
\ir ll_is_bitemporal_table.sql
\ir ll_check_bitemporal_update_conditions.sql
\ir ll_bitemporal_correction.sql
\ir ll_bitemporal_delete.sql
\ir ll_bitemporal_inactivate.sql
\ir ll_bitemporal_insert.sql
\ir ll_bitemporal_insert_select.sql
\ir ll_bitemporal_update.sql
\ir ll_bitemporal_update_select.sql
\ir ll_bitemporal_delete_select.sql
\ir ll_create_bitemporal_table_partitioned.sql
\ir ll_create_bitemporal_partition.sql
\ir ll_bitemporal_table_type.sql
\ir ll_is_data_type_correct.sql
\ir ll_bitemporal_correction_effective.sql

commit;

