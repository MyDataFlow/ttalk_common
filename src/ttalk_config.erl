-module(ttalk_config).
-export([start/0,get_plain_terms_file/1]).

-record(config, {key,value}).
-record(local_config, {key,value}).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec start() -> ok.
start() ->
    %% 创建配置管理表
    %% 使用内存模式
    mnesia:create_table(config,
                        [{ram_copies, [node()]},
                         {storage_properties,
                          [{ets, [{read_concurrency, true}]}]},
                         {attributes, record_info(fields, config)}]),
    %% 如果存在多节点则会将自己加入其中
    mnesia:add_table_copy(config, node(), ram_copies),
    %% 本地特有的配置
    mnesia:create_table(local_config,
                        [{ram_copies, [node()]},
                         {storage_properties,
                          [{ets, [{read_concurrency, true}]}]},
                         {local_content, true},
                         {attributes, record_info(fields, local_config)}]),
    mnesia:add_table_copy(local_config, node(), ram_copies),
    ok.
%%%===================================================================
%%% Internal functions
%%%===================================================================
get_plain_terms_file(File1) ->
    File = get_absolute_path(File1),
    case file:consult(File) of
        {ok, Terms} ->
            include_config_files(Terms);
        {error, {LineNumber, erl_parse, _ParseMessage} = Reason} ->
          exit({error,Reason});
        {error, Reason} ->
          exit({error,Reason})
    end.

-spec get_absolute_path(string()) -> string().
get_absolute_path(File) ->
  case filename:pathtype(File) of
    absolute ->
      File;
    relative ->
      {ok, Cwd} = file:get_cwd(),
      filename:absname_join(Cwd, File)
  end.

-spec include_config_files([term()]) -> [term()].
include_config_files(Terms) ->
    include_config_files(Terms, []).
include_config_files([], Res) ->
    Res;
include_config_files([{include_config_file, Filename} | Terms], Res) ->
    include_config_files([{include_config_file, Filename, []} | Terms], Res);
include_config_files([{include_config_file, Filename, Options} | Terms], Res) ->
    IncludedTerms = get_plain_terms_file(Filename),
    Disallow = proplists:get_value(disallow, Options, []),
    IncludedTerms2 = delete_disallowed(Disallow, IncludedTerms),
    AllowOnly = proplists:get_value(allow_only, Options, all),
    IncludedTerms3 = keep_only_allowed(AllowOnly, IncludedTerms2),
    include_config_files(Terms, Res ++ IncludedTerms3);
include_config_files([Term | Terms], Res) ->
    include_config_files(Terms, Res ++ [Term]).


%% @doc Filter from the list of terms the disallowed.
%% Returns a sublist of Terms without the ones which first element is
%% included in Disallowed.
-spec delete_disallowed(Disallowed :: [atom()],
                        Terms :: [term()]) -> [term()].
delete_disallowed(Disallowed, Terms) ->
    lists:foldl(
      fun(Dis, Ldis) ->
          delete_disallowed2(Dis, Ldis)
      end,
      Terms,
      Disallowed).


delete_disallowed2(Disallowed, [H | T]) ->
    case element(1, H) of
        Disallowed ->
            delete_disallowed2(Disallowed, T);
        _ ->
            [H | delete_disallowed2(Disallowed, T)]
    end;
delete_disallowed2(_, []) ->
    [].


%% @doc Keep from the list only the allowed terms.
%% Returns a sublist of Terms with only the ones which first element is
%% included in Allowed.
-spec keep_only_allowed(Allowed :: [atom()],
                        Terms :: [term()]) -> [term()].
keep_only_allowed(all, Terms) ->
    Terms;
keep_only_allowed(Allowed, Terms) ->
    {As, NAs} = lists:partition(
                  fun(Term) ->
                      lists:member(element(1, Term), Allowed)
                  end,
                  Terms),
    As.
