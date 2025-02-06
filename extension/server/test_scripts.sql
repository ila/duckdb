CREATE DECENTRALIZED TABLE medical_records (
       record_id      INT,
       patient_id     STRING RANDOMIZED,
       condition      STRING SENSITIVE,
       diagnosis      STRING SENSITIVE,
       treatment      STRING,
       hospital_id      INT MINIMUM AGGREGATION 10,
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
