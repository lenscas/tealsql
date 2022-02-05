fn main() {
    let base = pgteal::Base {};
    let lua = mlua::Lua::new();
    let globals = lua.globals();
    globals.set("base", base).unwrap();
    lua.load(
        "
    base.connect(\"postgres://tealsql:tealsql@localhost/tealsql\", function(con)
        local a = con:fetch_all(\"SELECT * FROM t_random\", {},1000)
        for _ in ipairs(a) do
        end
    end)
    ",
    )
    .exec()
    .unwrap();
}
