/* @name= get_all */
SELECT *
FROM everything;
/* @name= get_by_string */
SELECT *
FROM everything
WHERE "varchar1" = :getBy;