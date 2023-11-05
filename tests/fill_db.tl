local pgteal = require "libpgteal"
pgteal.connect("postgres://tealsql:tealsql@localhost/tealsql",function(con:pgteal.Connection):nil 
    con:execute("TRUNCATE TABLE public.t_random RESTART IDENTITY RESTRICT;",{})
    local toInsert = {}
    for i=1,1000000 do
        local startTimeDataCreation = os.clock()
        local textToInsert = ""
        for __=1,20 do
            textToInsert = textToInsert .. string.char(math.random(65, 65 + 25)):lower()
        end
        table.insert(toInsert,{textToInsert})
        if #toInsert >= 34400 then
            local endTimeDataCreation = os.clock()
            print("data creation time:", endTimeDataCreation - startTimeDataCreation)
            local startTime = os.clock()
            con:bulk_insert("t_random",{"text"}, toInsert)
            local endTime = os.clock()
            print("query time:", endTime - startTime)
            print("processed", i, "items")
            toInsert = {}
        end
    end
    if #toInsert > 0 then
        con:bulk_insert("t_random",{"text"}, toInsert)
    end

end)