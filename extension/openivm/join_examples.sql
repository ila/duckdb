/* Some example queries to try out joins in IVM.
 * Once these queries work, they could be added to the README of a similar-purpose file.
 */

-- Base tables.
CREATE TABLE gods (uid INT, user_name TEXT);
CREATE TABLE payments (from_uid INT, to_uid INT, amount INT);

-- Insert data.
INSERT INTO gods VALUES (1, 'Apollo'), (2, 'Artemis'), (3, 'Dionysus'), (4, 'Poseidon'), (5, 'Zeus');
INSERT INTO payments VALUES (1, 2, 1722), (2, 3, 53), (2, 5, 360), (3, 1, 80), (3, 2, 137), (3, 5, 83), (5, 1, 42), (1, 2, 222);

-- All payments, annotated with the sender's name.
CREATE MATERIALIZED VIEW named_payments AS
    SELECT g.user_name, p.*
    FROM gods AS g, payments AS p
    WHERE p.to_uid = g.uid;

SELECT * FROM named_payments;

-- All payments from uid 3, including the amount.
CREATE MATERIALIZED VIEW recipients AS
  SELECT g.user_name, p.amount
  FROM gods as g, payments as p
  WHERE p.from_uid = 3 AND g.uid = p.to_uid;

-- Sum of incoming payments made by a god.
-- Normally, the GROUP BY would be by ID, and then the projection is by username, but DuckDB does not like this.
-- Perhaps because of the lack of primary/foreign keys in this example.
-- TODO: Support primary key so that GROUP BY g.user_name does not have to be used, but g.user_id as g's PK instead.
CREATE MATERIALIZED VIEW income AS
  SELECT g.user_name, sum(p.amount)
  FROM gods as g, payments as p
  WHERE p.to_uid = g.uid
  GROUP BY g.user_name;  -- Replace by the more correct "GROUP BY g.uid" once primary keys are supported.


-- Add another payment (this time to Apollo)
INSERT INTO payments VALUES (3, 1, 30);

-- Now, update the IVM and see what happens.
PRAGMA ivm('named_payments');
PRAGMA ivm('recipients');
PRAGMA ivm('income');
