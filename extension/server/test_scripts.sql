CREATE DECENTRALIZED TABLE diagnoses (
    patient_id INTEGER RANDOMIZED,
    diagnosis_date DATE,
    diagnosis VARCHAR,
    patient_city VARCHAR MINIMUM AGGREGATION 3,
    doctor_id INTEGER SENSITIVE
);

CREATE DECENTRALIZED MATERIALIZED VIEW city_daily_covid_diagnoses AS
    SELECT diagnosis_date, patient_city
    FROM diagnoses
    WHERE diagnosis = 'COVID-19'
    WINDOW 24
    TTL 48;

select t1.diagnosis_date, t1.patient_city
from rdda_centralized_view_city_daily_covid_diagnoses t1
inner join
(select patient_city, rdda_window, count(distinct client_id) as clients
from rdda_centralized_view_city_daily_covid_diagnoses group by patient_city, rdda_window
having clients >= 3) t2
on t1.patient_city = t2.patient_city and t1.rdda_window = t2.rdda_window;

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
