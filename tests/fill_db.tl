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
            con:bulk_insert("t_random",{"text"}, toInsert, nil)
            local endTime = os.clock()
            print("query time:", endTime - startTime)
            print("processed", i, "items")
            toInsert = {}
        end
    end
    if #toInsert > 0 then
        con:bulk_insert("t_random",{"text"}, toInsert, nil)
    end
    con:execute("TRUNCATE TABLE public.everything RESTART IDENTITY RESTRICT;",{})
    con:insert(
        "everything",
        {
            varchar1= "someString",
            bigint1 = 2,
            character1 = "a",
            float41 = 2.1,
            money1=2,
            json1 = {
                a="b",
                c="d"
            },
            int4array = {1,2,3,4},
            interval1 = pgteal.interval(1,2,3) as number,
            uuid1 = "48361ec1-1094-47e3-b0dc-288fe759a17b"
        },
        nil
    )
    con:insert(
        "everything",
        {
            varchar1= "nice",
            bigint1 = 4,
            character1 = "a",
            float41 = 5.1,
            money1=123191,
            json1 = {
                a={"b","c",3},
                c="d"
            },
            int4array = {1,2,3,4},
            interval1 = pgteal.interval(0,0,12321312) as number,
            uuid1 = "6113f0e7-2029-428f-8852-f07a82405d83"
        },
        nil
    )

end)