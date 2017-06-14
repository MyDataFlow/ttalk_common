%%%-------------------------------------------------------------------
%% @doc ttalk_common public API
%% @end
%%%-------------------------------------------------------------------

-module(ttalk_common_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    ttalk_common_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================