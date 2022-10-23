fn main() {
    let base = pgteal::Base {};
    let lua = mlua::Lua::new();
    let globals = lua.globals();
    globals.set("base", base).unwrap();
    lua.load(
        "
    base.connect(\"postgres://tealsql:tealsql@localhost/tealsql\", function(con)
        local a = con:fetch_all_async(\"SELECT * FROM t_random\", {})
        for _ in a:iter() do
        end
    end)
    ",
    )
    .exec()
    .unwrap();
}
