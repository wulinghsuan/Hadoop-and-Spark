IMDB Queries:

1) Number of titles with duration superior than 2 hours.

nano two_hours.hql

SELECT COUNT(*)
FROM imdb_title_basics
WHERE runtimeminutes > 120;    

!run two_hours.hql

60446

2) Average duration of titles containing the string "world".

nano world.hql

SELECT AVG(runtimeminutes)
FROM imdb_title_basics
WHERE primaryTitle LIKE "%world%";

!run world.hql

43.581

3) Average rating of titles having the genre "Comedy"

nano comedy.hql

SELECT AVG(averageRating)
FROM imdb_title_ratings
JOIN imdb_title_basics ON imdb_title_ratings.tconst = imdb_title_basics.tconst
WHERE array_contains(imdb_title_basics.genres, "Comedy");

!run comedy.hql

6.97

4) Average rating of titles not having the genre "Comedy"

nano nocomedy.hql

SELECT AVG(averageRating)
FROM imdb_title_ratings
JOIN imdb_title_basics ON imdb_title_ratings.tconst = imdb_title_basics.tconst
WHERE NOT array_contains(imdb_title_basics.genres, "Comedy");

!run nocomedy.hql

6.886

5) Top 10 movies directed by Quentin Tarantino

SELECT basic.primarytitle, rating.averagerating 
FROM 
    (SELECT * FROM imdb_title_crew 
    LATERAL VIEW explode(director) exp as ndir) as crew
    JOIN imdb_name_basics as name ON crew.ndir=name.nconst
    JOIN imdb_title_ratings as rating on rating.tconst=crew.tconst   
    JOIN imdb_title_basics as basic on basic.tconst=crew.tconst
WHERE 
    name.primaryname="Quentin Tarantino"
    AND basic.titletype="movie"
ORDER BY rating.averagerating DESC
LIMIT 10;

For the last query, try it in two queries first if you want.
You'll see that you have to make a join on some array type. Hint: "explode"

SELECT imdb_title_basics.primarytitle, imdb_title_ratings.averagerating
