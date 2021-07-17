/* @name= test */
SELECT *
FROM characters;
/* @name= test2 */
SELECT *
FROM characters
WHERE id = :id;
/* @name= test3 */
SELECT *
FROM characters
WHERE id = :id2;