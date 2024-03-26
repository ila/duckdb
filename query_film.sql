create materialized view film_rating as select rating, count(*) from film where film_id > 100 group by rating;
