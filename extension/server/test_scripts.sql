CREATE DECENTRALIZED TABLE diagnoses (
    patient_id INTEGER RANDOMIZED,
    diagnosis_date DATE,
    diagnosis VARCHAR,
    patient_city VARCHAR,
    doctor_id INTEGER SENSITIVE
);

CREATE DECENTRALIZED MATERIALIZED VIEW city_daily_covid_diagnoses AS
    SELECT diagnosis_date, patient_city
    FROM diagnoses
    WHERE diagnosis = 'COVID-19'
    WINDOW 24
    TTL 48
    MINIMUM AGGREGATION 3
    REFRESH 12;

INSERT INTO diagnoses (patient_id, diagnosis_date, diagnosis, patient_city, doctor_id) VALUES
(1, '2024-02-01', 'COVID-19', 'New York', 201),
(2, '2024-02-01', 'Flu', 'New York', 202),
(3, '2024-02-02', 'COVID-19', 'New York', 203),
(4, '2024-02-02', 'Cold', 'New York', 204),
(5, '2024-02-03', 'COVID-19', 'New York', 205),
(6, '2024-02-03', 'Pneumonia', 'New York', 206),
(7, '2024-02-04', 'COVID-19', 'New York', 207),
(8, '2024-02-04', 'Asthma', 'New York', 208),
(9, '2024-02-05', 'COVID-19', 'New York', 209),
(10, '2024-02-05', 'Allergy', 'New York', 210);

insert into city_daily_covid_diagnoses
SELECT diagnosis_date, patient_city
FROM diagnoses
WHERE diagnosis = 'COVID-19';

INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Berlin', now(), now(), 1, 1, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Berlin', now(), now(), 1, 2, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Berlin', now(), now(), 1, 3, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Berlin', now(), now(), 1, 4, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Amsterdam', now(), now(), 1, 5, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Amsterdam', now(), now(), 1, 6, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Amsterdam', now(), now(), 1, 7, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Milan', now(), now(), 1, 8, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-17', 'Milan', now(), now(), 1, 9, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-18', 'Amsterdam', now(), now(), 2, 10, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-18', 'Amsterdam', now(), now(), 2, 11, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-18', 'Amsterdam', now(), now(), 2, 12, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-18', 'Berlin', now(), now(), 2, 13, 1);
INSERT INTO rdda_centralized_view_city_daily_covid_diagnoses values ('2025-02-18', 'Berlin', now(), now(), 2, 14, 1);
