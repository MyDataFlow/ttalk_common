-module(ttalk_config).
-export([start/0,load_file/1]).
-export([add_option/2,get_option/1,del_option/1]).
-record(config, {key,value}).

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
    ok.

add_option(Opt, Val) ->
  mnesia:transaction(fun() ->
    mnesia:write(#config{key   = Opt,value = Val})
  end).
del_option(Opt) ->
  mnesia:transaction(fun mnesia:delete/1, [{config, Opt}]).

get_option(Opt) ->
  case ets:lookup(config, Opt) of
    [#config{value = Val}] ->
      Val;
    _ ->
      undefined
  end.
-spec load_file(File :: string()) -> ok.
load_file(File) ->
    Res = parse_file(File),
    io:format("~p~n",[Res]),
    {atomic,ok} = mnesia:transaction(fun()->
      lists:foreach(fun({K,V}) ->
        C = #config{key = K,value = V},
        mnesia:write(C)
      end, Res)
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================
parse_file(ConfigFile) ->
    Terms = get_plain_terms_file(ConfigFile),
    replace_macros(Terms).

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


%% @doc Split Terms into normal terms and macro definitions.
-spec split_terms_macros(Terms :: [term()]) -> {[term()], [term()]}.
split_terms_macros(Terms) ->
  lists:foldl(fun split_terms_macros_fold/2, {[], []}, Terms).

-spec split_terms_macros_fold(any(), Acc) -> Acc when
        Acc :: {[term()], [{Key :: any(), Value :: any()}]}.
split_terms_macros_fold({define_macro, Key, Value} = Term, {TOs, Ms}) ->
  case is_atom(Key) and is_all_uppercase(Key) of
    true ->
      {TOs, Ms ++ [{Key, Value}]};
    false ->
      exit({macro_not_properly_defined, Term})
  end;
split_terms_macros_fold(Term, {TOs, Ms}) ->
  {TOs ++ [Term], Ms}.

-spec is_all_uppercase(atom()) -> boolean().
is_all_uppercase(Atom) ->
  String = erlang:atom_to_list(Atom),
  lists:all(
    fun(C) when C >= $a, C =< $z -> false;
    (_) -> true
  end, String).
%% @doc Recursively replace in Terms macro usages with the defined value.
-spec replace(Terms :: [term()],
              Macros :: [term()]) -> [term()].
replace([], _) ->
  [];
replace([Term | Terms], Macros) ->
  [replace_term(Term, Macros) | replace(Terms, Macros)].

replace_term(Key, Macros) when is_atom(Key) ->
  case is_all_uppercase(Key) of
    true ->
      case proplists:get_value(Key, Macros) of
        undefined -> exit({undefined_macro, Key});
        Value -> Value
      end;
    false ->
      Key
  end;
replace_term({use_macro, Key, Value}, Macros) ->
  proplists:get_value(Key, Macros, Value);
replace_term(Term, Macros) when is_list(Term) ->
  replace(Term, Macros);
replace_term(Term, Macros) when is_tuple(Term) ->
  List = erlang:tuple_to_list(Term),
  List2 = replace(List, Macros),
  erlang:list_to_tuple(List2);
replace_term(Term, _) ->
  Term.

-spec replace_macros(Terms :: [term()]) -> [term()].
replace_macros(Terms) ->
  {TermsOthers, Macros} = split_terms_macros(Terms),
  replace(TermsOthers, Macros).
