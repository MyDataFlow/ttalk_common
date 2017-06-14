-module(rdbms_sup).
-export([start_link/0,init/1]).
-export([add_pool/2,add_pool/3,remove_pool/1]).
-export([pool_proc/1]).

-define(DEFAULTS, [{host,"127.0.0.1"}
                  ,{port,5432}
                  ,{database, "postgres"}
                  ,{username, "postgres"}
                  ,{password,""}
                  ,{query_timeout,5000}
                  ,{start_interval,30000}
                  ,{max_connect_retries,5}
                  ,{keepalive_interval,60000}
                  ]).

-spec start_link() -> ignore | {error, term()} | {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    ok = application:ensure_started(worker_pool),
    catch ets:new(prepared_statements, [public, named_table, {read_concurrency, true}]),
    {ok, {{one_for_one, 5, 60}, []}}.

-spec add_pool(Pool :: rdbms_worker:pool(), Size :: non_neg_integer()) -> supervisor:startchild_ret().
add_pool(Pool,Size)->
  ChildSpec = pool_spec(Pool, Size,?DEFAULTS),
  supervisor:start_child(?MODULE, ChildSpec).

-spec add_pool(Pool :: rdbms_worker:pool(), Size :: non_neg_integer(), Settings :: term()) -> supervisor:startchild_ret().
add_pool(Pool,Size,Settings) ->
  Keys = proplists:get_keys(Settings),
  Others = lists:foldl(fun(Key,Acc)->
    lists:keydelete(Key,1,Acc)
  end,?DEFAULTS,Keys),
  Settings2 = lists:merge(Settings,Others),
  ChildSpec = pool_spec(Pool, Size,Settings2),
  supervisor:start_child(?MODULE, ChildSpec).

-spec remove_pool(Pool :: rdbms_worker:pool()) -> ok.
remove_pool(Pool) ->
    ok = supervisor:terminate_child(?MODULE, Pool),
    ok = supervisor:delete_child(?MODULE, Pool).

-spec pool_spec(Pool :: rdbms_worker:pool(), Size :: non_neg_integer(), Settings :: term() ) -> supervisor:child_spec().
pool_spec(Pool, Size,Settings) ->
    Opts = [{workers, Size}, {worker, {rdbms_worker, {Pool,Settings}}}, {pool_sup_shutdown, infinity}],
    {Pool, {wpool, start_pool, [pool_proc(Pool), Opts]}, transient, 2000, supervisor, dynamic}.

pool_proc(Pool)->
    erlang:list_to_atom("rdbms_pool_" ++ erlang:atom_to_list(Pool)).
