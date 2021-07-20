/* @name= test */
SELECT id,user_id
FROM characters;
/* @name= test2 */
SELECT id,user_id
FROM characters
WHERE id = :id;
/* @name= test3 */
SELECT id,user_id
FROM characters
WHERE id = :id2;