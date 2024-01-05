create table client_information(id ubigint, creation timestamp, last_update timestamp, last_result timestamp);
create table rdda_queries(view_name varchar primary key, tables varchar, query varchar);
