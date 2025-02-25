create table if not exists client_information(id ubigint primary key, creation timestamp, last_update timestamp);
create table if not exists client_refreshes(view_name varchar primary key, last_result timestamp);
