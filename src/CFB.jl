module CFB
using HTTP
using JSON3
using SQLite
using Dates
using TimeZones
using DataFrames

include("CfbdClient.jl")
include("fetch.jl")
greet() = print("Hello World!")

end # module CFB
