local pgteal = require "libpgteal"
local luasql = require "luasql.postgres"
local sql = luasql.postgres()

local luasqlCon = sql:connect("postgres://tealsql:tealsql@localhost/tealsql")
local function simulateWork(a:integer):integer
    for _ =0,100 do
        a = a + math.random(1,2)
    end
    return a
end
pgteal.connect("postgres://tealsql:tealsql@localhost/tealsql",function(con:pgteal.Connection):nil
    local query = "SELECT * as rand FROM t_random"
    local totalTimeTealSync = 0.
    local totalTimeTealAsync = 0.
    local totalTimeTealAsyncCustom = 0.
    local totalLuasql = 0.

    local loopedTimes = 0;
    for _ =1,10 do
        local timeNeeded = 0.0
        local startTime = 0.0
        local endTime = 0.0
        loopedTimes = loopedTimes +1;
        
        collectgarbage()
        startTime = os.clock()
        local a = con:fetch_all(query,{})
        for k,__ in ipairs(a) do
            simulateWork(k)
        end
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time teal sync: ",timeNeeded)
        totalTimeTealSync = totalTimeTealSync + timeNeeded
        collectgarbage()
        startTime = os.clock()
        local b = con:fetch_all_async(query,{}, nil)
        local k = 1
        for _ in b:iter() do
            simulateWork(k)
            print("Do stuff")
            k = k +1
        end
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time teal async: ",timeNeeded)
        totalTimeTealAsync = totalTimeTealAsync + timeNeeded
        collectgarbage()
        startTime = os.clock()
        local d = con:fetch_all_async(query,{},nil)
        k = 1
        d:loop_all(function(_):nil simulateWork(k) k = k + 1 return nil end)
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time teal async custom loop: ",timeNeeded)
        totalTimeTealAsyncCustom = totalTimeTealAsyncCustom + timeNeeded
        collectgarbage()
        startTime = os.clock()
        local c = luasqlCon:execute(query) as luasql.Cursor
        local res = c:fetch("s")
        k = 1
        simulateWork(k)
        while res do
            k = k + 1
            res = c:fetch("s")
            simulateWork(k)
            
        end
        endTime = os.clock()
        timeNeeded = endTime - startTime
        print("End time lua sql: ",timeNeeded)
        totalLuasql = totalLuasql +timeNeeded
    end
    print("Totals:")
    print("Total teal sync:", totalTimeTealSync / loopedTimes)
    print("Total teal async:", totalTimeTealAsync / loopedTimes)
    print("Total teal async custom:", totalTimeTealAsyncCustom / loopedTimes)
    print("Total lua sql:", totalLuasql / loopedTimes)
end)