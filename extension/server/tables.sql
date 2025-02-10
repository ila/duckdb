create table rdda_clients(id ubigint primary key, last_update timestamp, last_result timestamp, size integer);
create table rdda_settings(setting varchar primary key, value varchar);
create table rdda_tables(name varchar primary key, type tinyint, query varchar, is_view boolean);  -- this contains tables & views
create table rdda_table_constraints(decentralized_table_name varchar references rdda_tables(name), column_name varchar, rdda_randomized bool, rdda_sensitive bool, rdda_minimum_aggregation tinyint, primary key(decentralized_table_name, column_name));
create table rdda_view_constraints(view_name varchar references rdda_tables(name), rdda_window tinyint, rdda_ttl tinyint, primary key(view_name));