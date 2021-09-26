local pgteal = require "libpgteal"
local pool = pgteal.connect_pool("postgres://tealsql:tealsql@localhost/tealsql")

local type resType = ({string : string | integer | number | boolean | {any:any}})

function printTable(a: {any:any},tabs:integer)
    local z = {}
    for _=0,tabs do
        table.insert(z,"")
    end
    local tab = table.concat(z,"    ")
    for k,v in pairs(a) do
        if v is {any:any} then
            print(tab,k, " = {")
            printTable(v,tabs+1)
            print(tab,"}")
        else
            print(tab,k," = ",v)
        end
    end
end


pool:get_connection(function(x:pgteal.Connection):boolean
    local z,y = pcall(function(): resType return x:fetch_one("SELECT * FROM everything WHERE \"varchar1\" = 'nice'",{}) end);
    print(y);
    if z then
        for k,v in pairs(y) do
            local p = v as any;
            if p is {any:any} then
                print(k,"\n = {")
                printTable(p,1)
                print("}")
            else
                print(k,v)
            end
        end
        assert(y.character1 == "a")
    end
    print("")
    print("")
    -- local a,b = pcall(
    --     function():integer 
    --         return x:execute(
    --             "INSERT INTO everything (\"varchar1\",\"json1\") VALUES ($1,$2)",
    --             {
    --                 "test_insert",
    --                 {"amazing!"}
    --             }
    --         )
    --     end
    -- )
    -- print(a,b)
    return true
end)

