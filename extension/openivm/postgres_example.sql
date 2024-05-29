-- download PostgreSQL extension at: http://extensions.duckdb.org/v0.10.2/linux_amd64/postgres_scanner.duckdb_extension.gz
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
