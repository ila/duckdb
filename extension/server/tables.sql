create table rdda_clients(id ubigint primary key, creation timestamp, last_update timestamp);
create table rdda_settings(setting varchar primary key, value varchar);
create table rdda_tables(name varchar primary key, type tinyint, query varchar, is_view boolean);  -- this contains tables & views
create table rdda_clients_refreshes(id ubigint references rdda_clients(id), table_name varchar references rdda_tables(name), last_result timestamp);
create table rdda_table_constraints(table_name varchar references rdda_tables(name), column_name varchar, rdda_randomized bool, rdda_sensitive bool, rdda_protected bool, primary key(table_name, column_name));
create table rdda_view_constraints(view_name varchar references rdda_tables(name), rdda_window int, rdda_ttl tinyint, rdda_refresh tinyint, rdda_min_agg tinyint, primary key(view_name));
create table rdda_current_window(view_name varchar references rdda_tables(name), rdda_window int, primary key(view_name));