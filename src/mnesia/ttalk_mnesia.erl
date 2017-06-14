-module(ttalk_mnesia).
-export([ensure_mnesia/1]).
ensure_mnesia(CreateFun)->
  ok = application:ensure_started(mneisa),
	ensure_mnesia_dir(),
	case is_virgin_node() of
		true ->
        Result = (catch ensure_mnesia_schema()),
        case aborted_on_error(Result) of
          {atomic,ok}->
            aborted_on_error((catch CreateFun()));
          Res ->
              Res
        end;
		false ->
			aborted_on_error((catch init_mnesia()))
	end.

dir() -> mnesia:system_info(directory).
ensure_mnesia_dir() ->
    MnesiaDir = dir() ++ "/",
    case filelib:ensure_dir(MnesiaDir) of
      {error, Reason} ->
        throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
      ok ->
        ok
end.

ensure_mnesia_schema()->
  ok = application:stop(mnesia),
  ok = mnesia:create_schema([node()]),
  ok = application:ensure_started(mneisa),
  ok.

is_virgin_node() ->
  case prim_file:list_dir(dir()) of
    {error, enoent} ->
      true;
    {ok, []} ->
      true;
    {ok, _} ->
      false
  end.

init_mnesia()->
  ok = application:ensure_started(mneisa),
  mneisa:wait_for_tables(mnesia:system_info(local_tables),infinity).

aborted_on_error(Result)->
  case Result of
    {aborted, Reason}   ->
      {aborted, Reason};
    {'EXIT', Reason} ->
      {aborted, Reason};
    Res ->
      {atomic, Res}
  end.
