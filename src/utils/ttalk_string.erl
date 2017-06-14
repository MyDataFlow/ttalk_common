-module(ttalk_string).
-export([list2hex/1,bin2hex/1]).

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

