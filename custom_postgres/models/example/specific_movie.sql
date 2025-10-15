{% set film_title = 'Dunkirk' %}

SELECT * 
FROM {{ source('films') }}
WHERE title = '{{ film_title }}'