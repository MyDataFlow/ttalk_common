-module(ttalk_string).
-export([list2hex/1,bin2hex/1]).
-export([to_string/1,to_string/2]).

list2hex(L)->
  LH0 = lists:map(
    fun(X)-> erlang:integer_to_list(X,16)
  end, L),
  LH = lists:map(
    fun([X,Y])-> [X,Y];
       ([X])-> [$0,X]
  end, LH0),
  lists:flatten(LH).

bin2hex(B) ->
  L = erlang:binary_to_list(B),
  list2hex(L).


to_string(Val) when is_integer(Val) -> erlang:integer_to_binary(Val);
to_string(Val) when is_float(Val) -> erlang:list_to_binary(io_lib:format("~.2f", [Val]));
to_string(Val) when is_atom(Val) -> erlang:atom_to_binary(Val);
to_string(Val) when is_list(Val)  ->erlang:list_to_binary(Val);
to_string(Val)-> Val.

to_string(Format,Val) ->
to_string(io_lib:format(Format,[Val])).
