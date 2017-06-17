-module(rdbms_pgsql).

-include_lib("epgsql/include/epgsql.hrl").

-export([connect/2, disconnect/1, query/3, prepare/3, execute/4]).

-spec connect(Args :: any(), QueryTimeout :: non_neg_integer()) ->
                     {ok, Connection :: term()} | {error, Reason :: any()}.
connect(Settings, QueryTimeout) ->
    case epgsql:connect(db_opts(Settings)) of
        {ok, Pid} ->
            epgsql:squery(Pid, [<<"SET statement_timeout=">>, integer_to_binary(QueryTimeout)]),
            epgsql:squery(Pid, <<"SET standard_conforming_strings=off">>),
            {ok, Pid};
        Error ->
            Error
    end.

-spec disconnect(Connection :: epgsql:connection()) -> ok.
disconnect(Connection) ->
    epgsql:close(Connection).

-spec query(Connection :: term(), Query :: any(),
            Timeout :: infinity | non_neg_integer()) -> rdbms_worker:query_result().
query(Connection, Query, _Timeout) ->
    pgsql_to_odbc(epgsql:squery(Connection, Query)).

-spec prepare(Connection :: term(), Name :: atom(),
  Statement :: iodata()) -> {ok, term()} | {error, any()}.
prepare(Connection, Name, Statement) ->
    BinName = atom_to_binary(Name, latin1),
    ReplacedStatement = replace_question_marks(Statement),
    case epgsql:parse(Connection, BinName, ReplacedStatement, []) of
        {ok, _} -> {ok, BinName};
        Error   -> Error
    end.

-spec execute(Connection :: term(), StatementRef :: term(), Params :: [term()],
              Timeout :: infinity | non_neg_integer()) -> rdbms_worker:query_result().
execute(Connection, StatementRef, Params, _Timeout) ->
    pgsql_to_odbc(epgsql:prepared_query(Connection, StatementRef, Params)).

%% Helpers

-spec db_opts(Settings :: term()) -> [term()].
db_opts(Settings) ->
    Server = proplists:get_value(host,Settings),
    Port = proplists:get_value(port,Settings),
    DB = proplists:get_value(database,Settings),
    User = proplists:get_value(username,Settings),
    Pass = proplists:get_value(password,Settings),
    [
     {host, Server},
     {port, Port},
     {database, DB},
     {username, User},
     {password, Pass}
    ].

-spec pgsql_to_odbc(epgsql:reply(term())) -> rdbms_worker:query_result().
pgsql_to_odbc(Items) when is_list(Items) ->
    lists:reverse([pgsql_to_odbc(Item) || Item <- Items]);
pgsql_to_odbc({error, #error{codename = unique_violation}}) ->
    {error, duplicate_key};
pgsql_to_odbc({error, #error{message = Message}}) ->
    {error, Message};
pgsql_to_odbc({ok, Count}) ->
    {updated, Count};
pgsql_to_odbc({ok, _Columns, Rows}) ->
    {selected, Rows}.

-spec replace_question_marks(Statement :: iodata()) -> iodata().
replace_question_marks(Statement) when is_list(Statement) ->
    replace_question_marks(iolist_to_binary(Statement));
replace_question_marks(Statement) when is_binary(Statement) ->
    [Head | Parts] = binary:split(Statement, <<"?">>, [global]),
    Placeholders = [<<"$", (integer_to_binary(I))/binary>> ||
        I <- lists:seq(1, length(Parts))],
    PartsWithPlaceholders = lists:zipwith(fun(A, B) -> [A, B] end,
         Placeholders, Parts),
    [Head | PartsWithPlaceholders].
