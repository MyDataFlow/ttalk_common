-module(ttalk_common_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  application:ensure_started(crypto),
  ttalk_common_sup:start_link().

stop(_State) ->
    ok.
