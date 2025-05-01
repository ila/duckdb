-- download PostgreSQL extension at: http://extensions.duckdb.org/v0.10.3/linux_amd64/postgres_scanner.duckdb_extension.gz
-- move the extension and unzip it in the .duckdb/extensions folder


-- call ivm_demo('p', 'public', 'demo', '/home/ila/Code/duckdb/extension/openivm/postgres_example_count.sql');

CREATE TABLE example_table
(
    id    SERIAL PRIMARY KEY,
    mode  VARCHAR(1) NOT NULL,
    value INTEGER    NOT NULL
);

INSERT INTO example_table (mode, value)
VALUES ('a', 10),
       ('c', 20),
       ('b', 30),
       ('a', 40),
       ('d', 50),
       ('b', 60),
       ('e', 70),
       ('a', 80),
       ('b', 90),
       ('c', 100);

INSERT INTO delta_example_table (mode, value, _duckdb_ivm_multiplicity)
VALUES ('a', 200, 1),
       ('c', 20, 1),
       ('b', 30, 1),
       ('f', 40, 1),
       ('d', 50, 1),
       ('f', 60, 1),
       ('e', 70, 1),
       ('a', 80, 1),
       ('b', 90, 0),
       ('c', 100, 0);
