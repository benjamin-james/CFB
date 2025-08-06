
struct CfbdClient
    api_key::String
    db::SQLite.DB
    function CfbdClient(api_key::String; db_path::String=":memory:")
        new(api_key, SQLite.DB(db_path))
    end
end
