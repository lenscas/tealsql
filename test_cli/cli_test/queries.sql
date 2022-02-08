/* @name= get_all */
SELECT *
FROM everything;
/* @name= get_by_string */
SELECT *
FROM everything
WHERE "varchar1" = :getBy;
/* @name= insert_first_row */
INSERT INTO everything 
    (varchar1,bigint1,uuid1,character1,float41,money1,json1,int4array,interval1) 
VALUES
    ( 
        :varchar1 , 
        :bigint1 , 
        :uuid1 , 
        :character1 , 
        :float41 , 
        :money1 , 
        :json1 , 
        :int4array , 
        :interval1 
    ) ;
/* @name= update_row */
UPDATE everything
SET 
    bigint1=:bigint1 ,
    uuid1=:uuid1 ,
    character1=:character1 ,
    float41=:float41 ,
    money1=:money1 ,
    json1=:json1 ,
    int4array= :int4array , 
    interval1= :interval1 
WHERE "varchar1"= :varchar1;
/* @name= delete_row */
DELETE FROM everything
WHERE "varchar1"= :varchar1;