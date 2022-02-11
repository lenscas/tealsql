/* 
    @name= get_all
    @create_fetch_one= false
    @create_fetch_optional= false
    @create_execute = false
*/
SELECT *
FROM everything;
/*
    this query gets a single row where "varchar1" = ':getBy'
    @name= get_by_string
    @create_fetch_all= false
*/
SELECT *
FROM everything
WHERE "varchar1" = :getBy;

/*
    @name= insert_first_row
    @create_fetch_one= false
    @create_fetch_optional= false
    @create_fetch_all= false
*/
INSERT INTO everything 
    (varchar1,bigint1,uuid1,character1,float41,money1,json1,int4array,interval1) 
VALUES
    ( 
        :varchar1,:bigint1,:uuid1 ,:character1 ,:float41 ,:money1 ,:json1 ,:int4array ,:interval1 
    ) ;
/* @name=update_row */
UPDATE everything
SET 
    bigint1=:bigint1,
    uuid1=:uuid1,
    character1=:character1,
    float41=:float41 ,
    money1=:money1 ,
    json1=:json1 ,
    int4array= :int4array, 
    interval1= :interval1
WHERE "varchar1"= :varchar1;

/* @name= delete_row */
DELETE FROM everything
WHERE "varchar1"=:varchar1;

/* @name = stress_test */
SELECT "\:varchar2" as "varchar2"
FROM
    (
        SELECT "varchar1" as "\:varchar2"
        FROM everything as "\\\:varchar3"
        WHERE "\\\:varchar3"."varchar1" = :varchar1
    ) as v;