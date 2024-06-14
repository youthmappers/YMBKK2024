COPY
(
    SELECT
        country_iso_a3,
        year,
        count(distinct user_id) as n_users,
        count(*) as n_edits
    FROM (
    	SELECT user_id, unnest(country_iso_a3) as country_iso_a3, year
    	FROM read_parquet("/data/processing/partitioned-ohsome-parquet-100997/*/*/*.parquet", hive_partitioning=1)
    	WHERE year = 2023
    ) foo
    GROUP BY year, country_iso_a3
    ORDER BY country_iso_a3, year
    )
TO 'mappers_per_country_2023.csv' (HEADER, DELIMITER ',');
