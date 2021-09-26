local pgteal = require "libpgteal"


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

local pool = pgteal.connect_pool("postgres://tealsql:tealsql@localhost/tealsql")

local type resType = ({string : string | integer | number | boolean | {any:any}})



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
    print("------")
    print("now the short hands")
    print("-----------")
    print("insert:",x:insert("everything",{varchar1="epic!",money1=24444001}))
    local res = x:fetch_one("SELECT * FROM everything WHERE \"varchar1\"=$1",{"epic!"});
    assert(res.money1 == 24444001)
    print("update:",x:update("everything",{varchar1="epic!"},{money1=0}))
    res = x:fetch_one("SELECT * FROM everything WHERE \"varchar1\"=$1",{"epic!"});
    assert(res.money1 == 0)
    print("delete:",x:delete("everything",{varchar1="epic!"}))
    assert(x:fetch_one("SELECT COUNT(*) as counter FROM everything WHERE \"varchar1\"=$1",{"epic!"}).counter == 0);
    return true
end)

