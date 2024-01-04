movies = LOAD '$M' USING PigStorage(',') AS (movie_id: int, year: int, title: chararray);
ratings = LOAD '$R' USING PigStorage(',') AS (movie_id: int, user_id: int, rating: int, rating_date: chararray);
joined_data = JOIN movies BY movie_id, ratings BY movie_id;
movie_ratings = FOREACH joined_data GENERATE movies::year AS year, movies::title AS title, (double)ratings::rating AS rating;
grouped_data = GROUP movie_ratings BY (year, title);
avg_ratings = FOREACH grouped_data GENERATE FLATTEN(group) AS (year: int, title: chararray), AVG(movie_ratings.rating) AS avg_rating;
avg_ratings = FOREACH avg_ratings GENERATE CONCAT(CONCAT((chararray)year, ': '), title) AS year_title, avg_rating;
STORE avg_ratings INTO '$O' USING PigStorage('\t');



