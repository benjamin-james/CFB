using HTTP
using JSON3
using SQLite
using Dates
using TimeZones
using DataFrames

struct CfbdClient
    api_key::String
    db_path::String
    function CfbdClient(api_key::String; db_path::String=":memory:")
        new(api_key, db_path)
    end
end

function get_url(client::CfbdClient, url::String)
    headers = ["Authorization" => "Bearer $(client.api_key)",
               "Accept" => "application/json"]
    try
        response = HTTP.get("https://api.collegefootballdata.com/$(url)", headers)
        if response.status == 200
            return JSON3.read(response.body)
        else
            println("Error: HTTP status code $(response.status)")
            return nothing
        end
    catch e
        println("An error occurred: $e")
        return nothing
    end
end

function fill_results(client::CfbdClient; since::Union{DateTime,Nothing} = nothing)
    db = SQLite.DB(client.db_path);
    table_names = [table_schema.name for table_schema in SQLite.tables(db)]
    if since !== nothing
        epoch = datetime2unix(since)
    elseif "Game" in table_names && "Calendar" in table_names && "Play" in table_names
        ef = DataFrame(SQLite.DBInterface.execute(db, """
SELECT * FROM Game
JOIN Calendar ON Game.week = Calendar.week AND Game.season = Calendar.season AND Game.seasonType = Calendar.seasonType
JOIN Play ON Play.gameId = Game.id
        WHERE Game.completed ORDER BY startDateUnix DESC LIMIT 1;"""));
        since = ZonedDateTime(ef.startDate[1])
        epoch = ef.startDateUnix[1]
    else
        since = DateTime(2001, 1, 1)
        epoch = datetime2unix(since)
    end
    for season in range(Dates.year(since), Dates.year(now()))
        println(season)
        get_calendar(client, season)
        get_games(client, season)
        cal = DataFrame(SQLite.DBInterface.execute(db, "SELECT * FROM Calendar WHERE Calendar.season == $(season) AND Calendar.startDateUnix >= $(epoch);"))
        for row in eachrow(cal)
            get_plays(client, row.season, row.week, seasonType=row.seasonType)
        end
    end
end
function get_calendar(client::CfbdClient, season::Int)
    results = get_url(client, "calendar?year=$(season)")
    if results === nothing
        return nothing
    end
    db = SQLite.DB(client.db_path)
    SQLite.execute(db, "BEGIN TRANSACTION")
    SQLite.execute(db, """
CREATE TABLE IF NOT EXISTS Calendar (
    season INTEGER NOT NULL,
    week INTEGER NOT NULL CHECK (week >= 1),
    seasonType TEXT NOT NULL,
    startDate TEXT NOT NULL,
    endDate TEXT NOT NULL,
    startDateUnix INTEGER NOT NULL,
    endDateUnix INTEGER NOT NULL,
    PRIMARY KEY (season, week, seasonType)
);""")
    stmt = SQLite.Stmt(db, "INSERT OR REPLACE INTO Calendar (season, week, seasonType, startDate, endDate, startDateUnix, endDateUnix) VALUES (?, ?, ?, ?, ?, ?, ?)")
    for item in results
        snix = Int64(floor(datetime2unix(ZonedDateTime(item.startDate).utc_datetime)))
        enix = Int64(floor(datetime2unix(ZonedDateTime(item.endDate).utc_datetime)))
        SQLite.execute(stmt, (item.season, item.week, item.seasonType, item.startDate, item.endDate, snix, enix))
    end
    SQLite.execute(db, "COMMIT TRANSACTION")
end

function get_games(client::CfbdClient, season::Int)
    results = get_url(client, "games?year=$(season)")
    if results === nothing
        return nothing
    end
    db = SQLite.DB(client.db_path)
    SQLite.execute(db, "BEGIN TRANSACTION")
    SQLite.execute(db, """
CREATE TABLE IF NOT EXISTS Game (
       id INTEGER PRIMARY KEY NOT NULL,
       season INTEGER NOT NULL,
       week INTEGER NOT NULL,
       seasonType TEXT NOT NULL,
       startDate TEXT NOT NULL,
       startDateUnix INTEGER NOT NULL,
       startTimeTBD INTEGER NOT NULL CHECK (startTimeTBD IN (0, 1)),
       completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
       neutralSite INTEGER NOT NULL CHECK (neutralSite IN (0, 1)),
       conferenceGame INTEGER NOT NULL CHECK (conferenceGame IN (0, 1)),
       attendance INTEGER CHECK (attendance >= 0),
       venueId INTEGER,
       venue TEXT,
       homeId INTEGER NOT NULL,
       homeTeam TEXT NOT NULL,
       homeConference TEXT,
       homeClassification TEXT,
       homePoints INTEGER CHECK (homePoints >= 0),
       homeLineScores TEXT,
       homePostgameWinProbability REAL,
       homePregameElo INTEGER,
       homePostgameElo INTEGER,
       awayId INTEGER NOT NULL,
       awayTeam TEXT NOT NULL,
       awayConference TEXT,
       awayClassification TEXT,
       awayPoints INTEGER CHECK (awayPoints >= 0),
       awayLineScores TEXT,
       awayPostgameWinProbability REAL,
       awayPregameElo INTEGER,
       awayPostgameElo INTEGER,
       excitementIndex REAL,
       highlights TEXT,
       notes TEXT
);""")
    stmt = SQLite.Stmt(db, "INSERT OR REPLACE INTO Game (id, season, week, seasonType, startDate, startDateUnix, startTimeTBD, completed, neutralSite, conferenceGame, attendance, venueId, venue, homeId, homeTeam, homeClassification, homeConference, homePoints, homeLineScores, homePostgameWinProbability, homePregameElo, homePostgameElo, awayId, awayTeam, awayClassification, awayConference, awayPoints, awayLineScores, awayPostgameWinProbability, awayPregameElo, awayPostgameElo, excitementIndex, highlights, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    for item in results
        snix = Int64(floor(datetime2unix(ZonedDateTime(item.startDate).utc_datetime)))
        away_line_scores = item.awayLineScores
        home_line_scores = item.homeLineScores
        if home_line_scores !== nothing
            home_line_scores = join(home_line_scores, ", ")
        end
        if away_line_scores !== nothing
            away_line_scores = join(away_line_scores, ", ")
        end
        SQLite.execute(stmt, (item.id, item.season, item.week, item.seasonType, item.startDate, snix, item.startTimeTBD, item.completed, item.neutralSite, item.conferenceGame, item.attendance, item.venueId, item.venue, item.homeId, item.homeTeam, item.homeClassification, item.homeConference, item.homePoints, home_line_scores, item.homePostgameWinProbability, item.homePregameElo, item.homePostgameElo, item.awayId, item.awayTeam, item.awayClassification, item.awayConference, item.awayPoints, away_line_scores, item.awayPostgameWinProbability, item.awayPregameElo, item.awayPostgameElo, item.excitementIndex, item.highlights, item.notes))
    end
    SQLite.execute(db, "COMMIT TRANSACTION")
    close(db)
end

function get_plays(client::CfbdClient, season::Int, week::Int; seasonType::Union{String,Nothing}=nothing)
    path = "plays?year=$(season)&week=$(week)"
    if seasonType !== nothing
        path = "$(path)&seasonType=$(seasonType)"
    end
    results = get_url(client, path)
    if results === nothing
        return nothing
    end
    db = SQLite.DB(client.db_path)
    SQLite.execute(db, "BEGIN TRANSACTION")
    SQLite.execute(db, """
CREATE TABLE IF NOT EXISTS Play (
    id TEXT PRIMARY KEY NOT NULL,
    driveId TEXT NOT NULL,
    gameId INTEGER NOT NULL CHECK (gameId >= 0),
    driveNumber INTEGER CHECK (driveNumber >= 0),
    playNumber INTEGER CHECK (playNumber >= 0),
    offense TEXT NOT NULL,
    offenseConference TEXT,
    offenseScore INTEGER NOT NULL,
    defense TEXT NOT NULL,
    defenseConference TEXT,
    defenseScore INTEGER NOT NULL,
    home TEXT NOT NULL,
    away TEXT NOT NULL,
    period INTEGER NOT NULL,
    clock_minutes INTEGER,
    clock_seconds INTEGER,
    offenseTimeouts INTEGER,
    defenseTimeouts INTEGER,
    yardline INTEGER NOT NULL,
    yardsToGoal INTEGER NOT NULL,
    down INTEGER NOT NULL,
    distance INTEGER NOT NULL,
    yardsGained INTEGER NOT NULL,
    scoring INTEGER NOT NULL CHECK (scoring IN (0, 1)),
    playType TEXT NOT NULL,
    playText TEXT,
    ppa REAL,
    wallclock TEXT
);""")
    stmt = SQLite.Stmt(db, "INSERT OR REPLACE INTO Play (gameId, driveId, id, driveNumber, playNumber, offense, offenseConference, offenseScore, defense, defenseConference, defenseScore, home, away, period, clock_minutes, clock_seconds, offenseTimeouts, defenseTimeouts, yardline, yardsToGoal, down, distance, yardsGained, scoring, playType, playText, ppa, wallclock) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    for item in results
        SQLite.execute(stmt, (item.gameId, item.driveId, item.id, item.driveNumber, item.playNumber, item.offense, item.offenseConference, item.offenseScore, item.defense, item.defenseConference, item.defenseScore, item.home, item.away, item.period, item.clock.minutes, item.clock.seconds, item.offenseTimeouts, item.defenseTimeouts, item.yardline, item.yardsToGoal, item.down, item.distance, item.yardsGained, item.scoring, item.playType, item.playText, item.ppa, item.wallclock))
    end
    SQLite.execute(db, "COMMIT TRANSACTION")
end

