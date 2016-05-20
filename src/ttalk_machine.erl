-module(ttalk_machine).
-export([start/0]).
-export([id/0,id/2]).
-export([name/1]).
-export([hw_addrs/0,hw_addr/1]).

-type nodeid() :: non_neg_integer().
-record(ttalk_machine, {
		  name :: atom(),
		  id :: nodeid()
         }).

%% 跟新Mneisa中的Node的ID
start() ->	
    mnesia:create_table(ttalk_machine,
            [{ram_copies, [node()]},
             {type, set},
             {attributes, record_info(fields, ttalk_machine)}]),
	mnesia:add_table_copy(ttalk_machine, node(), ram_copies),
	register_node(node()),
	ok.

%% 每个节点都适用mnesia存储一个自身的ID
%% 当任何进程查询的时候，都会缓存到进程字典中
%% @doc Return an integer node ID.
-spec id() -> {ok, nodeid()}.
id() ->
    %% Save result into the process's memory space.
    case get(node_id) of
        undefined ->
            {ok, NodeId} = select_node_id(node()),
            put(node_id, NodeId),
            {ok, NodeId};
        NodeId ->
            {ok, NodeId}
    end.
-spec register_node(atom()) -> 'ok'.
register_node(NodeName) ->
	{atomic, _} = mnesia:transaction(
					fun() ->
							case mnesia:read(ttalk_machine, NodeName) of
								[] ->
									mnesia:write(#ttalk_machine{name = NodeName, id = next_node_id()});
								[_] -> ok
							end
					end),
    ok.

-spec next_node_id() -> nodeid().
next_node_id() ->
    max_node_id() + 1.
%% 使用Mnesia进行foldl找到最大的id
-spec max_node_id() -> nodeid().
max_node_id() ->
    mnesia:foldl(fun(#ttalk_machine{id=Id}, Max) -> max(Id, Max) end, 0, ttalk_machine).

-spec select_node_id(NodeName :: atom()) -> {'error','not_found'} | {'ok',nodeid()}.
select_node_id(NodeName) ->
    case mnesia:dirty_read(ttalk_machine, NodeName) of
        [#ttalk_machine{id=Id}] -> {ok, Id};
        [] -> {error, not_found}
    end.

-spec name(atom()) -> binary().
name(hostname)->
	{ok,HostName} = inet:gethostname(),
	unicode:characters_to_binary(HostName);
name(node) ->
	Node = node(),
	erlang:atom_to_binary(Node,utf8).

-spec id(atom(),atom()|list())-> {ok,nodeid()}.
id(hw_addr,default)->	
	HwAddrs = hw_addrs(),
	First = lists:nth(1,HwAddrs),
	{ok,NodeId} = hw_addr_to_integer(First), 
	{ok,NodeId};
id(hw_addr,Interface) ->	
	HwAddr = hw_addr(Interface),
	{ok,NodeId} = hw_addr_to_integer(HwAddr),
	{ok,NodeId}.


hw_addrs()->
	{ok,Interfaces} = inet:getifaddrs(),
	lists:filtermap(
	  fun({Iface, Opts})->
			  HwAddr = proplists:get_value(hwaddr, Opts),
			  case HwAddr of
				  undefined ->
					  false;
				  [0,0,0,0,0,0] ->
					  false;
				  _ ->
					  {true,{Iface,HwAddr}}
			  end
	  end,Interfaces).

hw_addr(Interface)->
	HwAddrs = hw_addrs(),
	HwAddr = proplists:get_value(Interface,HwAddrs),
	{Interface,HwAddr}.

hw_addr_to_integer({_Inteface,undefined})->
	{error,undefined};
hw_addr_to_integer({_Inteface,HwAddr})->				
	Bin = erlang:list_to_binary(HwAddr),
	<<Int:48/big-unsigned-integer>> = Bin,
	{ok,Int}.


