{% macro generate_film_ratings() %}

WITH films_with_ratings AS
(
    SELECT
        film_id,
        title,
        release_date,
        price,
        rating,
        user_rating,
        CASE
            WHEN user_rating >= 4.0 THEN 'Must watch'
            WHEN user_rating >= 3.0 THEN 'Should watch'
            WHEN user_rating >= 2.0 THEN 'Can watch'
            WHEN user_rating >= 1.0 THEN 'Can miss'
            ELSE 'Should not watch'
        END AS rating_category
    FROM {{ source('destination_db', 'films') }}
),
films_with_actors AS 
(
    SELECT 
        f.film_id,
        f.title,
        STRING_AGG(a.actor_name, ',') AS actors
    FROM {{ source('destination_db', 'films') }} f
    LEFT JOIN {{ source('destination_db', 'film_actors') }} fa ON f.film_id = fa.film_id
    LEFT JOIN {{ source('destination_db', 'actors') }} a ON fa.actor_id = a.actor_id
    GROUP BY f.film_id, f.title
)
SELECT 
    fwr.*,
    fwa.actors
FROM films_with_ratings fwr
LEFT JOIN films_with_actors fwa ON fwr.film_id = fwa.film_id

{% endmacro %}