

local record tealsql
    
--Tealsql is a sql library made to be easy and safe to use. Its RAII centric API prevents mistakes like forgetting to close connections.
        --The library also makes prepared statements easy to use as it does the binding of parameters for you.
        --
        --There are also several helper functions to do basic tasks like deleting, updating or inserting values. These allow you to quickly get something going without the need to write SQL for these basic operation
        --
        --
        --
        --Lastly, this library is made with teal in mind. This means that a automatically generated `.d.tl` file is shipped with the dependency. Allowing teal users to always have correct type information about this library.
        --
        --Further more: This library also has a CLI that acts similar to pgtyped but for teal. This gives teal users the ability to write totally type safe sql queries.
        --
        --

    --The value used to represent `null` values in json.
        null:any


    --Connect to the server and create a connection pool
        --
        --## Params:
        --
        --- connection_string:The string used to connect to the server.
        --
        --## Example:
        --
        --```teal_lua
        --local pool = tealsql.connect_pool("postgres://userName:password@host/database")
        --local res:{string:integer} = pool:get_connection(function(con:tealsql.Connection):{string:integer}
        --    return con:fetch_one("SELECT $1 as test",{2}) as {string:integer}
        --end)
        --assert(res.test ==  2)
        --```
        --
        connect_pool:function(string) : Pool


    --Returns a json string representing the definitions of this library
        --
        --This can be used to generate online documentation
        --
        --## Params:
        --
        --- pretty: If the json needs to be pretty printed or not
        gen_json:function(boolean) : string


    --Connect to the server and create a single connection
        --
        --## Params:
        --
        --- connection_string: The string used to connect to the server.
        --
        --- func: The function that will be executed after the connection has been made.
        --
        --This function receives the connection object, which will be cleaned up after the function has been executed.
        --
        --A value returned from this function will also be returned by the connect function
        --
        --## Example:
        --
        --```teal_lua
        --local res: {string:integer} = tealsql.connect("postgres://userName:password@host/database",function(con:tealsql.Connection):{string:integer}
        --    return con:fetch_one("SELECT $1 as test",{2}) as {string:integer}
        --end)
        --assert(res.test ==  2)
        --```
        --
        connect:function<Res>(string , function(Connection) : (Res...)) : (Res...)


    --Returns the value used to represent `null` values in json.
        nul:function() : any


    --Creates the interval type from postgresql.
        --
        --## Params:
        --
        --- months: The amount of months in this interval. Defaults to 0
        --
        --- days: The amount of days in this interval. Defaults to 0
        --
        --- microseconds: The amount of microseconds in this interval. Defaults to 0
        interval:function(integer , integer , integer) : Interval

        help:function(string) : string



    interface  Pool
        is userdata
    
--A connection pool containing at most 10 connections.
        --
        --Opening a database connection for each and every operation to the database can quickly become expensive.
        --
        --A connection pool is a standard technique that can manage opening and re-using connections. Normally it also enforces a maximum number of connections as these are an expensive resource on the database server.
        --
        --

    --Gets a connection from the pool
        --
        --Parameters:
        --
        --call_back: The function that will be executed after the connection has been made.
        --
        --This function receives the connection object, which will be cleaned up after the function has been executed.
        --
        --A value returned from this function will also be returned by the connect function
        get_connection:function<Res>(self : Pool , function(Connection) : (Res...)) : (Res...)

        help:function(string) : string


    end
    

    interface  Connection
        
    
--A single database connection
        --
        --

    --Fetches 1 or 0 results from the database
        --
        --## Params:
        --
        --- query: The query string that needs to be executed
        --
        --- params: An array (table) containing the parameters that this function needs
        fetch_optional:function(self : Connection , query : string , params : { any }) : ({ string : any } )


    --Fetches all results into a table
        --
        --## Params:
        --
        --- query: The query string that needs to be executed
        --
        --- params: An array (table) containing the parameters that this function needs
        fetch_all:function(self : Connection , query : string , params : { any }) : ({ { string : any }  })


    --Runs a thread in the background that fetches all results. Allowing you to consume the results in batches, or do other things while the query is being executed
        --
        --# Params:
        --
        --- query: The query string that needs to be executed
        --
        --- params: An array (table) containing the parameters that this function needs
        --
        --- chunk_count: How big the batches are that will be returned from the background thread to the main one. Higher batch count may improve performance
        fetch_all_async:function(self : Connection , query : string , params : { any } , chunk_count : integer) : Stream<any>


    --Fetches exactly 1 value from the database.
        --
        --## Params:
        --
        --- query: The query string that needs to be executed
        --
        --- params: An array (table) containing the parameters that this function needs
        execute:function(self : Connection , query : string , params : { any }) : integer


    --## Params:
        --
        --- query: The query string that needs to be executed
        --
        --- params: An array (table) containing the parameters that this function needs
        fetch_one:function(self : Connection , query : string , params : { any }) : ({ string : any } )


    --A shorthand to run a basic insert command.
        --
        --# WARNING!:
        --
        --the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.
        --
        --The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.
        --
        --## Parameters:
        --
        --- name: the table name that will be inserted into
        --
        --- values: A table where the keys are the column names and the values are the values that will be inserted
        --
        --- needs_to_get_quoted: If the table name should get quotes around it. Defaults to false, set to true if the name contains .'s
        insert:function(self : Connection , name : string , values : { string : any }  , needs_to_get_quoted : boolean) : integer


    --A shorthand to run a basic bulk insert command.
        --
        --# WARNING!:
        --
        --the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.
        --
        --The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.
        --
        --## Parameters:
        --
        --- name: the table name that will be inserted into
        --
        --- columns: the columns that the query will insert into
        --
        --- values: an table containing a table for every row. Columns are entirely decided by order
        --
        --- needs_to_get_quoted: If the table name should get quotes around it. Defaults to false, set to true if the name contains .'s
        bulk_insert:function(self : Connection , name : string , columns : { string } , values : { { any } } , needs_to_get_quoted : boolean) : integer


    --A shorthand to run a basic update command.
        --
        --# WARNING!:
        --
        --the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these.
        --
        --The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.
        --
        --## Parameters:
        --
        --- name: the table name that will be inserted into
        --
        --- old_values: A table used to construct the `where` part of the query. The keys are the column names and the values are the values that will be matched against
        --
        --- new_values: A table where the keys are the column names and the values are the values that this column will be updated to
        --
        --- needs_to_get_quoted: If the table name should get quotes around it. Defaults to false, set to true if the name contains .'s
        update:function(self : Connection , name : string , old_values : { string : any }  , new_values : { string : any }  , needs_to_get_quoted : boolean) : integer


    --A simple shorthand to do an insert and specify how the row needs to be updated instead if there is a conflict on the given index
        --
        --WARNING!:
        --
        --the table, index and column names are NOT escaped. SQL injection IS possible if user input is allowed for these.
        --
        --The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.
        --
        --Parameters:
        --
        --- name: the table name that will be inserted into
        --
        --- values: A table used to construct the `where` part of the query. The keys are the column names and the values are the values that will be matched against
        --
        --- new_values: A table where the keys are the column names and the values are the values that this column will be updated to
        --
        --- needs_to_get_quoted: If the table name should get quotes around it. Defaults to false, set to true if the name contains .'s
        upsert:function(self : Connection , name : string , values : { string : any }  , index : string , to_replace : { string : any }  , needs_to_get_quoted : boolean) : integer


    --A shorthand to run a basic delete command.
        --
        --WARNING!:
        --
        --the table and column names are NOT escaped. SQL injection IS possible if user input is allowed for these values.
        --
        --The values that get inserted ARE properly escaped. For these, SQL injection is NOT possible.
        --
        --Parameters:
        --
        --- name: the table name that will be inserted into
        --
        --- old_values: A table used to construct the `where` part of the query. The keys are the column names and the values are the values that will be matched against
        --
        --- needs_to_get_quoted: If the table name should get quotes around it. Defaults to false, set to true if the name contains .'s
        delete:function(self : Connection , name : string , check_on : { string : any }  , needs_to_get_quoted : boolean) : integer


    --Starts a new transaction.
        --
        --## Params:
        --
        --- func: The function that will be executed after the transaction has been made.
        --
        --This function can return 2 values, the first is a boolean that determines if the transaction should be committed or not.
        --
        --The second can be of any type and will be returned as is
        --
        --After this function is executed the transaction will either be committed or rolled back.
        --
        --It will be rolled back if the callback threw an error, or returned false for the first return value
        --
        --Otherwise, it will be committed
        --
        --## Examples:
        --
        --### Committing
        --
        --```teal_lua
        --tealsql.connect("postgres://userName:password@host/database",function(con:tealsql.Connection):nil
        --    local success, res:boolean, integer = con:begin(function(con:tealsql.Connection):(boolean,integer)
        --        con:execute("INSERT INTO some_table (some_column) VALUES (1)", nil);
        --        return true, 1
        --    end)
        --    assert(success)
        --    assert(res ==  1)
        --end)
        --
        --```
        --
        --### Manual Rollback
        --
        --```teal_lua
        --tealsql.connect("postgres://userName:password@host/database",function(con:tealsql.Connection):nil
        --    local success, res: boolean, integer = con:begin(function(con:tealsql.Connection):(boolean,integer)
        --        con:execute("INSERT INTO some_table (some_column) VALUES (1)", nil);
        --        return false, 1
        --    end)
        --    assert(not success)
        --    assert(res ==  1)
        --end)
        --```
        --
        --### Rollback on error
        --
        --```teal_lua
        --tealsql.connect("postgres://userName:password@host/database",function(con:tealsql.Connection):nil
        --    local success, res: boolean, integer = con:begin(function(con:tealsql.Connection):(boolean,integer)
        --        con:execute("INSERT INTO some_table (some_column) VALUES (1)", nil);
        --        error("This will also cause a rollback")
        --    end)
        --    --we will never reach this part, as the error gets rethrowed
        --    assert(res ==  1)
        --end)
        --
        --
        --```
        --        
        begin:function<Res>(self : Connection , function(Connection): ((boolean ) , Res...)): ((boolean ) , Res...)

        help:function(string) : string


    end
    

    interface  Stream<Res>
        is userdata
    
--Returned from connection:fetch_all_async(). It allows you to do other things while the query is running in a background thread.
        --
        --

    --Constructs a blocking iterator that will loop over all the items.
        iter:function(self : Stream<Res>): ((function(Stream<Res>) : Res ) , (Stream<Res>))


    --returns the next item if it is available or nill if not.
        --
        --Does NOT block the main thread.
        try_next:function(self : Stream<Res>): ((boolean ) , (Res))


    --Waits until the next item is available and then returns it.
        --
        --DOES block the main thread
        next:function(self : Stream<Res>) : Res

        loop_all:function<Out>(self : Stream<Res> , function(Res) : Out) : ({ Out })

        help:function(string) : string


    end
    

    interface  Interval
        
    
--
        months:integer

        days:integer

        microseconds:integer


    end
    
end


return tealsql