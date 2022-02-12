local pgteal = require "libpgteal"
local queries = require "queries_sql"

local function compareFloats(a:number,b:number, key: any):boolean, string
    local threshold =  0.0000000500000
    local diff = math.abs(a - b)
    if diff > threshold then
        return false, "missmatch at key " .. tostring(key) .. " left: " .. tostring(a) .." right: " .. tostring(b) .. " it is " .. diff - threshold .. " too big"
    end
    return true
end

local function checkTableEqual(a:{any:any},b:{any:any}):boolean,string
    for k,v in pairs(a) do
        local inb = b[k]
        if v is table then
            if inb is table then
                local res, message = checkTableEqual(v,inb)
                if not res then
                    return false, message
                end
            else
                return false, "missmatch at key " .. tostring(k) .. " left: " .. tostring(v) .." right: " .. tostring(inb)
            end
        else
            if v is number and inb is number then
                local res,message = compareFloats(v, inb,k)
                if not res then
                    return res,message
                end
            else
                local res = v == inb
                if not res then
                    return false , "missmatch at key " .. tostring(k) .. " left: " .. tostring(v) .." right: " .. tostring(inb)
                end
            end
        end
    end
    return true
end

local function test(connection:pgteal.Connection):boolean
    local insert:queries.insert_first_row.InsertFirstRowIn = {
        varchar1 = "some_test",
        bigint1 = 1,
        uuid1 = "123e4567-e89b-12d3-a456-426614174000",
        character1 = "a",
        float41 = 1.2,
        money1 = 300,
        json1 = {name="some amazing name",fields={1,"string"}},
        int4array = {1,2,3,4},
        interval1 = pgteal.interval(0,2,3)
    }
    print("preparing DB to be in known state")
    queries.delete_row.execute({varchar1=insert.varchar1},connection)
    print("inserting row to DB")
    queries.insert_first_row
        .execute(insert,connection)
    print("fetch one result")
    local res = queries.get_by_string.fetch_one({getBy="some_test"},connection)
    print("Comparing against insert")
    assert(checkTableEqual(res as {any:any},insert as {any:any}))
    
    print("update row")
    local update:queries.update_row.UpdateRowIn = {
        varchar1 = "some_test",
        bigint1 = 2,
        uuid1 = "123e4567-e89b-12d3-a456-426614174000",
        character1 = "a",
        float41 = 2.2,
        money1 = 301,
        json1 = {name="some amazing name 2",fields={1,"string 2"}},
        int4array = {1,2,3,4,5,6,7},
        interval1 = pgteal.interval(1,3,8)
    }
    queries.update_row.execute(update,connection)
    print("getting updated row")
    local res3 = queries.get_by_string.fetch_one({getBy="some_test"},connection)
    print("Comparing against update")
    assert(checkTableEqual(res3 as {any:any},update as {any:any}))
    print("deleting row")
    queries.delete_row.execute({varchar1=insert.varchar1},connection)
    print("getting deleted row")
    local res2 = queries.get_by_string.fetch_optional({getBy="some_test"},connection)
    print("Checking for nill")
    assert(res2 == nil, "row was not deleted. Got back: " .. tostring(res2))
    print("make transaction that fails by throwing error")
    pcall(
        function():boolean,integer
            return connection:begin(
                function(con:connection):(boolean,integer)
                    queries.insert_first_row.execute(insert,con)
                    error("some error")
                end
            ) 
        end
    )
    print("try get value from failed transaction by error")
    local res4 = queries.get_by_string.fetch_optional({getBy="some_test"},connection)
    print("Checking for nill")
    assert(res4 == nil, "transaction was not rolledback. Got back: " .. tostring(res4))
    print("make transaction that fails by returning false")
    
    local success, value = connection:begin(
        function(con:connection):(boolean,integer)
            queries.insert_first_row.execute(insert,con)
            return false, 1
        end
    )
    assert(not success, "failed transaction returned true")
    assert(value == 1, "transaction did not return correct value. Got" .. tostring(value))
    print("try get value from failed transaction by error")
    local res5 = queries.get_by_string.fetch_optional({getBy="some_test"},connection)
    print("Checking for nill")
    assert(res5 == nil, "transaction was not rolledback. Got back: " .. tostring(res5))

    print("try with successful transaction")
    local success2, value2 = connection:begin(
        function(con:connection):(boolean,integer)
            queries.insert_first_row.execute(insert,con)
            return true, 2
        end
    )
    assert(success2,"transaction failed while needing to succeed")
    assert(value2 == 2,"successful transaction did not return correct value. Got " .. tostring(2))
    print("Trying to get insert from successful transaction")
    local res6 = queries.get_by_string.fetch_one({getBy="some_test"},connection)
    print("Comparing against insert")
    assert(checkTableEqual(res6 as {any:any},insert as {any:any}))
    print("cleaning up")
    queries.delete_row.execute({varchar1 = insert.varchar1},connection)
    return true
end

local connectionString  = "postgres://tealsql:tealsql@localhost/tealsql"

print("Start test with normal connection")

pgteal.connect(connectionString,function(connection:pgteal.Connection):nil
    assert(test(connection) == true,"test function did not return true")
    print("Successfully gone over the test!")
end)

print("Start test with pooled connection")

local pool =  pgteal.connect_pool(connectionString)
pool:get_connection(function(connection:pgteal.Connection):nil
    assert(test(connection) == true,"test function did not return true")
    print("Successfully gone over the test!")
end)

print("Check if the disabled functions have not been generated")

local get_all = queries.get_all as {string:any}
assert(get_all.fetch_all,"fetch_all for queries.get_all did not get generated")
assert(not get_all.execute,"execute for queries.get_all got generated")
assert(not get_all.fetch_one,"fetch_one for queries.get_all got generated")
assert(not get_all.fetch_optional,"fetch_optional for queries.get_all got generated")
print("functions for queries.get_all successfully generated")

print("Check functions for insert_first_row")
local insert_first_row = queries.insert_first_row as {string:any}
assert(not insert_first_row.fetch_all,"fetch_all for queries.insert_first_row did not get generated")
assert(insert_first_row.execute,"execute for queries.insert_first_row got generated")
assert(not insert_first_row.fetch_one,"fetch_one for queries.insert_first_row got generated")
assert(not insert_first_row.fetch_optional,"fetch_optional for queries.insert_first_row got generated")

print("compile check to make sure that combined types are seen as the same")

local output_get_all : queries.get_all.ThisOutputName = {}
local output_get_by_string : queries.get_by_string.ThisOutputName = {}
output_get_all = output_get_by_string

print("Both tests succeeded!")