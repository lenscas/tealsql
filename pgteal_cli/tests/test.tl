local tests = require"test_sql"
local db = require"tealsql"

local pool = db.Base.connectPool("nice")
pool:get_connection(function(connection):boolean,boolean
    local con = connection as db.Connection
    local a = tests.test.fetch_one(nil,con)
    local b = tests.test.execute(nil,con)
    return true, false
end)
