local pgteal = require "libpgteal"
local luasql = require "luasql.postgres"
local sql = luasql.postgres()

local luasqlCon = sql:connect("postgres://tealsql:tealsql@localhost/tealsql")
pgteal.connect("postgres://tealsql:tealsql@localhost/tealsql",function(con:pgteal.Connection):nil
    local totalTimeTealSync = 0.
    local totalTimeTealAsync = 0.
    local totalLuasql = 0.

    local loopedTimes = 0;
    for _ =1,10 do
        loopedTimes = loopedTimes +1;
        collectgarbage()
        local startTime = os.clock()
        local a = con:fetch_all("SELECT * FROM t_random",{})
        for _,__ in ipairs(a) do
        end
        local endTime = os.clock()
        local timeNeeded = endTime - startTime
        print("End time teal sync: ",timeNeeded)
        totalTimeTealSync = totalTimeTealSync + timeNeeded
        collectgarbage()
        startTime = os.clock()
        local b = con:fetch_all_async("SELECT * FROM t_random",{})
        for _ in b:iter() do
        end
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time teal async: ",timeNeeded)
        totalTimeTealAsync = totalTimeTealAsync + timeNeeded
        collectgarbage()
        startTime = os.clock()
        local c = luasqlCon:execute("SELECT * FROM t_random") as luasql.Cursor
        local res = c:fetch("s")
        while res do
            res = c:fetch("s")
        end
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time lua sql: ",timeNeeded)
        totalLuasql = totalLuasql +timeNeeded
    end
    print("Totals:")
    print("Total teal sync:", totalTimeTealSync / loopedTimes)
    print("Total teal async:", totalTimeTealAsync / loopedTimes)
    print("Total lua sql:", totalLuasql / loopedTimes)
end)