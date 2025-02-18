CREATE DECENTRALIZED TABLE medical_records (
    record_id      INT,
    patient_id     STRING RANDOMIZED,
    condition      STRING SENSITIVE,
    diagnosis      STRING SENSITIVE,
    treatment      STRING,
    hospital_id    INT MINIMUM AGGREGATION 10,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE DECENTRALIZED MATERIALIZED VIEW total_visits AS
    SELECT patient_id, COUNT(*) as visits
    FROM medical_records
    GROUP BY patient_id
    WINDOW 24
    TTL 48;

CREATE REPLICATED TABLE hospitals (
    hospital_id    INT,
    name           STRING,
    location       STRING
);

CREATE CENTRALIZED TABLE contracts (
    contract_id    INT,
    hospital_id    INT,
    insurance_id   INT,
    start_date     DATE,
    end_date       DATE
);

CREATE CENTRALIZED MATERIALIZED VIEW hospital_contracts AS
SELECT hospital_id, COUNT(*) as contracts
FROM contracts
GROUP BY hospital_id;

CREATE TABLE mv_test (
    c1 STRING,
    c2 STRING,
    c3 STRING,
    sum_steps INT,
    win int,
    action bool,
    client_id STRING
);

insert into mv_test values ('a', 'b', 'c', 1, 1, true, 'client1');
insert into mv_test values ('a', 'b', 'c', 2, 1, true, 'client2');
insert into mv_test values ('a', 'b', 'b', 3, 1, true, 'client3');

select c1, c2, c3, sum_steps
from mv_test
where c3 in (
    select c3 from (
        select c3, win, count(distinct client_id) as clients
        from mv_test
        group by c3, win
        )
    where clients > 1
    );
