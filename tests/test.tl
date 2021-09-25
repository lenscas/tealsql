local pgteal = require "libpgteal"
local pool = pgteal.connect_pool("postgres://tealsql:tealsql@localhost/tealsql")

local type resType = ({string : string | integer | number | boolean})

pool:get_connection(function(x:pgteal.Connection):boolean
    local z,y = pcall(function(): resType return x:fetch_one("SELECT * FROM everything",{}) end);
    print(y);
    if z then
        print(y.character1)
        assert(y.character1 == "a")
    end
    return true
end)