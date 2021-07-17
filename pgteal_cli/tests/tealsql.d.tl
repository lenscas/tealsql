local record tealsql
        record Base
                userdata
                -- Pure functions
                connectPool: function(string):(Pool)

        end
        record Pool
                userdata
                -- Pure methods
                get_connection: function(Pool, function(...:any):any...):(boolean, any)

        end
        record Connection
                userdata
                -- Pure methods
                fetch_optional: function(Connection, string, any):(any)
                fetch_all: function(Connection, string, any):({any})
                execute: function(Connection, string, any):(integer)
                fetch_one: function(Connection, string, any):(any)
                -- Mutating methods
                begin: function(Connection, function(...:any):any...):(boolean, any)

        end
end
return tealsql