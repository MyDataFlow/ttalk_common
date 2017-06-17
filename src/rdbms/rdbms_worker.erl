%%%-------------------------------------------------------------------
%%% @author David Gao <>
%%% @copyright (C) 2017, David Gao
%%% @doc
%%%
%%% @end
%%% Created : 11 Jun 2017 by David Gao <>
%%%-------------------------------------------------------------------
-module(rdbms_worker).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
				 terminate/2, code_change/3]).

-export([prepare/2,execute/3,execute/4,sql_query/2,
	sql_query/3,sql_transaction/2,sql_transaction/3]).

-type pool() :: atom().
-export_type([pool/0]).

-type odbc_msg() :: {sql_query, _} | {sql_transaction, fun()} | {sql_execute, atom(), iodata()}.

-type single_query_result() :: {selected, [tuple()]} |
                               {updated, non_neg_integer() | undefined} |
                               {error, Reason :: string() | duplicate_key}.
-type query_result() :: single_query_result() | [single_query_result()].
-type transaction_result() :: {aborted, _} | {atomic, _} | {error, _}.

-export_type([query_result/0]).

-define(STATE_KEY,rdbms_state).
-define(MAX_TRANSACTION_RESTARTS, 10).
-define(TRANSACTION_TIMEOUT, 60000). % milliseconds
-define(KEEPALIVE_QUERY, <<"SELECT 1;">>).
-define(BEGIN_TRANSACTION, <<"BEGIN;">>).
-define(ROLLBACK_TRANSACTION,<<"ROLLBACK;">>).
-define(COMMIT_TRANSACTION,<<"COMMIT;">>).

-define(SERVER, ?MODULE).

-record(state, {
	pool,
	settings,
	db_ref,
	prepared = #{} :: #{binary() => term()}
}).
-type state() :: #state{}.
%%%===================================================================
%%% API
%%%===================================================================
-spec prepare(Name, Statement :: iodata()) ->
	{ok, Name} | {error, already_exists} when Name :: atom().
prepare(Name, Statement) when is_binary(Name) ->
    case ets:insert_new(prepared_statements, {Name,Statement}) of
        true  -> {ok, Name};
        false -> {error, already_exists}
    end.

-spec execute(Pool :: pool(), Name :: binary(), Parameters :: [term()]) ->query_result().
execute(Pool, Name, Parameters) when is_atom(Name), is_list(Parameters) ->
    execute(Pool, Name, Parameters,?TRANSACTION_TIMEOUT).

-spec execute(Pool :: pool(), Name :: binary(),
		Parameters :: [term()],Timeout :: non_neg_integer()) ->query_result().
execute(Pool, Name, Parameters,Timeout) when is_atom(Name), is_list(Parameters) ->
	 sql_call(Pool, {sql_execute, Name, Parameters},Timeout).

-spec sql_query(Pool :: pool(), Query :: any()) -> query_result().
sql_query(Pool, Query) ->
	sql_query(Pool, Query,?TRANSACTION_TIMEOUT).
-spec sql_query(Pool :: pool(), Query :: any(),
	Timeout :: non_neg_integer()) -> query_result().
sql_query(Pool, Query,Timeout) ->
	sql_call(Pool, {sql_query, Query},Timeout).

%% @doc SQL transaction based on a list of queries
-spec sql_transaction(pool(), fun() | maybe_improper_list()) -> transaction_result().
sql_transaction(Pool, Queries) when is_list(Queries) ->
	F = fun() -> lists:map(fun sql_query_t/1, Queries) end,
  sql_transaction(Pool, F);
%% SQL transaction, based on a erlang anonymous function (F = fun)
sql_transaction(Pool, F) when is_function(F) ->
	sql_transaction(Pool, F,?TRANSACTION_TIMEOUT).

-spec sql_transaction(pool(), fun() | maybe_improper_list(),
	Timeout :: non_neg_integer()) -> transaction_result().
sql_transaction(Pool, Queries,Timeout) when is_list(Queries) ->
	F = fun() -> lists:map(fun sql_query_t/1, Queries) end,
  sql_transaction(Pool, F,Timeout);
sql_transaction(Pool, F,Timeout) when is_function(F) ->
	    sql_call(Pool, {sql_transaction, F},Timeout).

sql_query_t(Query) ->
	sql_query_t(Query, get(?STATE_KEY)).
sql_query_t(Query, State) ->
	QRes = sql_query_internal(Query, State),
	case QRes of
		{error, Reason} ->
			throw({aborted, Reason});
	   _ when is_list(QRes) ->
	    case lists:keysearch(error, 1, QRes) of
				{value, {error, Reason}} ->
					throw({aborted, Reason});
	       _ ->
	        QRes
	     end;
	   _ ->
	   	QRes
	 end.
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Pool,Settings}) ->
		process_flag(trap_exit, true),
		State = #state{
			pool = Pool,
			settings = Settings
		},
		QueryTimeout = proplists:get_value(query_timeout,Settings),
		StartInterval = proplists:get_value(start_interval,Settings),
		ConnectRetries = proplists:get_value(connect_retries,Settings),
		KeepaliveInterval = proplists:get_value(keepalive_interval,Settings),
		case connect(Settings,ConnectRetries, 2, StartInterval,QueryTimeout) of
			 {ok, DBRef} ->
				 schedule_keepalive(KeepaliveInterval),
				 {ok, State#state{db_ref = DBRef}};
			 Error ->
					 {stop, Error}
	 end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({sql_cmd, Command}, From, State) ->
    {Result,NewState } =run_sql_cmd(Command, State),
		{reply, Result,NewState};
handle_call(_Request, _From, State) ->
		Reply = ok,
		{reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
		{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(keepalive,#state{settings = Settings } = State) ->
    case sql_query_internal([?KEEPALIVE_QUERY], State) of
        {selected, _} ->
						KeepaliveInterval = proplists:get_value(keepalive_interval,Settings),
            schedule_keepalive(KeepaliveInterval),
            {noreply, State};
        {error, _} = Error ->
            {stop, {keepalive_failed, Error}, State}
    end;
handle_info({'EXIT', _Pid, _Reason} = Reason, State) ->
    {stop, Reason, State};
handle_info(_Info, State) ->
		{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{db_ref = DBRef}) ->
    catch rdbms_pgsql:disconnect(DBRef).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
		{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec connect(Settings :: term(), Retry :: non_neg_integer(), RetryAfter :: non_neg_integer(),
              MaxRetryDelay :: non_neg_integer(),QueryTimeout :: non_neg_integer()) -> {ok, term()} | {error, any()}.
connect(Settings, Retry, RetryAfter, MaxRetryDelay,QueryTimeout) ->
    case rdbms_pgsql:connect(Settings,QueryTimeout) of
        {ok, _} = Ok ->
            Ok;
        Error when Retry =:= 0 ->
            Error;
        _Error ->
            SleepFor = rand:uniform(RetryAfter),
            timer:sleep(timer:seconds(SleepFor)),
            NextRetryDelay = RetryAfter * RetryAfter,
            connect(Settings, Retry - 1, max(MaxRetryDelay, NextRetryDelay), MaxRetryDelay,QueryTimeout)
    end.

-spec schedule_keepalive(KeepaliveInterval :: non_neg_integer()) -> any().
schedule_keepalive(KeepaliveInterval) ->
	if
			KeepaliveInterval > 0 ->
	 				erlang:send_after(KeepaliveInterval, self(), keepalive);
			true ->
					ok
	end.

sql_query_internal(Query, #state{db_ref = DBRef}) ->
	rdbms_pgsql:query(DBRef, Query).

-spec sql_execute(Name :: atom(), Params :: [term()], state()) -> {query_result(), state()}.
sql_execute(Name, Params, State = #state{db_ref = DBRef,settings = Settings}) ->
    {StatementRef, NewState} = prepare_statement(Name, State),
		QueryTimeout = proplists:get_value(query_timeout,Settings),
    Res = rdbms_pgsql:execute(DBRef, StatementRef, Params, QueryTimeout),
    {Res, NewState}.

-spec prepare_statement(Name :: atom(), state()) -> {Ref :: term(), state()}.
prepare_statement(Name, State = #state{db_ref = DBRef, prepared = Prepared}) ->
	    case maps:get(Name, Prepared, undefined) of
	        undefined ->
	            [{_, Statement}] = ets:lookup(prepared_statements, Name),
	            {ok, Ref} = rdbms_pgsql:prepare(DBRef, Name, Statement),
	            {Ref, State#state{prepared = maps:put(Name, Ref, Prepared)}};
	        Ref ->
	            {Ref, State}
	    end.
-spec outer_transaction(F :: fun(),
                        NRestarts :: 0..10,
                        Reason :: any(), state()) -> {transaction_result(), state()}.
outer_transaction(F, NRestarts, _Reason, State) ->
    sql_query_internal([?BEGIN_TRANSACTION], State),
    put(?STATE_KEY, State),
    Result = (catch F()),
    erase(?STATE_KEY), % Explicitly ignore state changed inside transaction
    case Result of
        {aborted, Reason} when NRestarts > 0 ->
            sql_query_internal([?ROLLBACK_TRANSACTION], State),
            outer_transaction(F, NRestarts - 1, Reason, State);
        {aborted, Reason} when NRestarts =:= 0 ->
            %% Too many retries of outer transaction.
            sql_query_internal([?ROLLBACK_TRANSACTION], State),
            {{aborted, Reason}, State};
        {'EXIT', Reason} ->
            %% Abort sql transaction on EXIT from outer txn only.
            sql_query_internal([?ROLLBACK_TRANSACTION], State),
            {{aborted, Reason}, State};
        Res ->
            %% Commit successful outer txn
            sql_query_internal([?COMMIT_TRANSACTION], State),
            {{atomic, Res}, State}
    end.

-spec inner_transaction(fun(), state()) -> transaction_result() | {'EXIT', any()}.
inner_transaction(F, _State) ->
	case catch F() of
		{aborted, Reason} ->
	  	{aborted, Reason};
	  {'EXIT', Reason} ->
	    {'EXIT', Reason};
	  {atomic, Res} ->
	    {atomic, Res};
	  Res ->
	    {atomic, Res}
	end.

run_sql_cmd({sql_query, Query}, State) ->
		    {sql_query_internal(Query, State), State};
run_sql_cmd({sql_transaction, F}, State) ->
		    outer_transaction(F, ?MAX_TRANSACTION_RESTARTS, "", State);
run_sql_cmd({sql_execute, Name, Params}, State) ->
		    sql_execute(Name, Params, State).

-spec run_sql_transaction(odbc_msg(), state()) -> any().
run_sql_transaction({sql_query, Query}, State) ->
	{sql_query_internal(Query, State), State};
run_sql_transaction({sql_transaction, F}, State) ->
	inner_transaction(F, State);
run_sql_transaction({sql_execute, Name, Params}, State) ->
	sql_execute(Name, Params, State).

-spec sql_call(Pool :: pool(), Msg :: odbc_msg(),
	Timeout :: non_neg_integer()) -> any().
sql_call(Pool, Msg,Timeout) ->
    case get(?STATE_KEY) of
        undefined ->
					run_sql(Pool, Msg,Timeout);
        State     ->
            {Res, NewState} = run_sql_transaction(Msg, State),
            put(?STATE_KEY, NewState),
            Res
    end.


-spec run_sql(Pool :: pool(), Msg :: odbc_msg(),
	Timeout :: non_neg_integer()) -> any().
run_sql(Pool, Msg,Timeout) ->
    PoolProc = rdbms_sup:pool_proc(Pool),
    case whereis(PoolProc) of
        undefined ->
					{error, {no_odbc_pool, PoolProc}};
        _ ->
          wpool:call(PoolProc, {sql_cmd, Msg}, best_worker, Timeout)
    end.
