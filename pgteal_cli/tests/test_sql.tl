local Connection = require("tealsql").Connection
local type TestIn = nil
local type TestOut = record
    id : integer
    user_id : integer
    dungeon : string
    character_state : string
    current_event : string
end
local type Test2In = record
    id : integer
end
local type Test2Out = record
    id : integer
    user_id : integer
    dungeon : string
    character_state : string
    current_event : string
end
local type Test3In = record
    id2 : integer
end
local type Test3Out = record
    id : integer
    user_id : integer
    dungeon : string
    character_state : string
    current_event : string
end
return {    test = {
        TestIn = TestIn,
        TestOut= TestOut,
                  fetch_all = function (params: TestIn, connection: Connection): {TestOut}
		local param_order:{string} = {

}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_all(
			[[
SELECT *
FROM characters]],
			query_params
		) as {TestOut}
		end,
                              fetch_one = function (params: TestIn, connection: Connection): TestOut
		local param_order:{string} = {

}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_one(
			[[
SELECT *
FROM characters]],
			query_params
		) as TestOut
		end,
                              fetch_optional = function (params: TestIn, connection: Connection): TestOut
		local param_order:{string} = {

}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_optional(
			[[
SELECT *
FROM characters]],
			query_params
		) as TestOut
		end,
                              execute = function (params: TestIn, connection: Connection): integer
		local param_order:{string} = {

}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:execute(
			[[
SELECT *
FROM characters]],
			query_params
		) as integer
		end},    test2 = {
        Test2In = Test2In,
        Test2Out= Test2Out,
                  fetch_all = function (params: Test2In, connection: Connection): {Test2Out}
		local param_order:{string} = {
    "id"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_all(
			[[
SELECT *
FROM characters
WHERE id = $1]],
			query_params
		) as {Test2Out}
		end,
                              fetch_one = function (params: Test2In, connection: Connection): Test2Out
		local param_order:{string} = {
    "id"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_one(
			[[
SELECT *
FROM characters
WHERE id = $1]],
			query_params
		) as Test2Out
		end,
                              fetch_optional = function (params: Test2In, connection: Connection): Test2Out
		local param_order:{string} = {
    "id"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_optional(
			[[
SELECT *
FROM characters
WHERE id = $1]],
			query_params
		) as Test2Out
		end,
                              execute = function (params: Test2In, connection: Connection): integer
		local param_order:{string} = {
    "id"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:execute(
			[[
SELECT *
FROM characters
WHERE id = $1]],
			query_params
		) as integer
		end},    test3 = {
        Test3In = Test3In,
        Test3Out= Test3Out,
                  fetch_all = function (params: Test3In, connection: Connection): {Test3Out}
		local param_order:{string} = {
    "id2"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_all(
			[[
SELECT *
FROM characters
WHERE id = $1;]],
			query_params
		) as {Test3Out}
		end,
                              fetch_one = function (params: Test3In, connection: Connection): Test3Out
		local param_order:{string} = {
    "id2"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_one(
			[[
SELECT *
FROM characters
WHERE id = $1;]],
			query_params
		) as Test3Out
		end,
                              fetch_optional = function (params: Test3In, connection: Connection): Test3Out
		local param_order:{string} = {
    "id2"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:fetch_optional(
			[[
SELECT *
FROM characters
WHERE id = $1;]],
			query_params
		) as Test3Out
		end,
                              execute = function (params: Test3In, connection: Connection): integer
		local param_order:{string} = {
    "id2"
}
		local query_params = {}
		for k,v in ipairs(param_order) do
			query_params[k] = (params as {string:any})[v]
		end
		return connection:execute(
			[[
SELECT *
FROM characters
WHERE id = $1;]],
			query_params
		) as integer
		end}}