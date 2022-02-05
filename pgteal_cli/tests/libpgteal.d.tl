local record libpgteal
	-- Base

		-- Pure functions
		connect_pool: function(string):(Pool)
		connect: function<Res>(string, function(Connection):(Res)):(Res)
		nul: function():(any)
		interval: function(integer, integer, integer):(Interval)
		-- Meta functions
		metamethod __index: function(string):(any)


	record Pool
		userdata
		-- Pure methods
		get_connection: function<Res>(Pool, function(Connection):(Res)):(boolean, Res)

	end
	record Connection
		userdata
		-- Pure methods
		fetch_optional: function(Connection, string, {integer:string | {any:any} | integer | number | boolean}):({string:string | {any:any} | integer | number | boolean})
		fetch_all: function(Connection, string, {integer:string | {any:any} | integer | number | boolean}):({{string:string | {any:any} | integer | number | boolean}})
		fetch_all_async: function(Connection, string, {integer:string | {any:any} | integer | number | boolean}, integer):(Stream<any>)
		execute: function(Connection, string, {integer:string | {any:any} | integer | number | boolean}):(integer)
		fetch_one: function(Connection, string, {integer:string | {any:any} | integer | number | boolean}):({string:string | {any:any} | integer | number | boolean})
		-- Mutating methods
		begin: function<Res>(Connection, function(Connection):(boolean,Res)):(boolean, Res)

	end
	record Stream<any>
		userdata
		-- Pure methods
		iter: function(Stream<any>):(function(Stream<any>):(any), {any:any})
		-- Mutating methods
		try_next: function(Stream<any>):(any)
		next: function(Stream<any>):(any)

	end
	record Interval
		
		-- Fields
		months : integer
		days : integer
		microseconds : integer

	end
end
return libpgteal
