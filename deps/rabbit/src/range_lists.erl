-module(range_lists).

-export([new/0]).
-export([from_list/1]).
-export([to_list/1]).
-export([merge/2]).
-export([subtract/2]).
-export([subtract_range/2]).

%% @todo Make proper tests.
-export([test/0]).

-type range() :: {integer(), integer()}.
-type range_list() :: [range()].

-export_type([range_list/0]).

new() ->
    [].

-spec from_list([integer()]) -> range_list().

from_list([]) ->
    [];
from_list(List0) ->
    [Lo|List] = lists:usort(List0),
    {Hi, Tail} = from_list_hi(List, Lo),
    [{Lo, Hi}|from_list(Tail)].

from_list_hi([El|Tail], Hi) when El =:= Hi + 1 ->
    from_list_hi(Tail, Hi + 1);
from_list_hi(Tail, Hi) ->
    {Hi, Tail}.

-spec to_list(range_list()) -> [integer()].

to_list([]) ->
    [];
to_list([{Lo, Hi}|Tail]) ->
    seq_loop(Hi - Lo + 1, Hi, to_list(Tail)).

%% Taken from lists.erl in OTP master from before 24.0
%% (commit c96ddce67d72f0d1990d2994f18187a0eec9c534).
seq_loop(N, X, L) when N >= 4 ->
     seq_loop(N-4, X-4, [X-3,X-2,X-1,X|L]);
seq_loop(N, X, L) when N >= 2 ->
     seq_loop(N-2, X-2, [X-1,X|L]);
seq_loop(1, X, L) ->
     [X|L];
seq_loop(0, _, L) ->
     L.

-spec merge(RangeList, RangeList) -> RangeList
    when RangeList :: range_list().

merge([], RangeList) ->
    RangeList;
merge(RangeList, []) ->
    RangeList;
%% One is exclusively smaller and not contiguous to the other.
merge([L = {_, LHi}|LTail], [R = {RLo, _}|RTail]) when LHi + 1 < RLo ->
    [L|merge(LTail, [R|RTail])];
merge([L = {LLo, _}|LTail], [R = {_, RHi}|RTail]) when RHi + 1 < LLo ->
    [R|merge([L|LTail], RTail)];
%% One is inclusively encompassing the other.
merge([L = {LLo, LHi}|LTail], [{RLo, RHi}|RTail]) when LLo =< RLo, LHi >= RHi ->
    merge([L|LTail], RTail);
merge([{LLo, LHi}|LTail], [R = {RLo, RHi}|RTail]) when RLo =< LLo, RHi >= LHi ->
    merge(LTail, [R|RTail]);
%% One is overlapping or contiguous to the other.
merge([{LLo, _}|LTail], [{RLo, RHi}|RTail]) when LLo =< RLo -> %% LHi < RHi
    merge([{LLo, RHi}|LTail], RTail);
merge([{_, LHi}|LTail], [{RLo, _}|RTail]) -> %% RLo =< LLo, RHi < LHi
    merge([{RLo, LHi}|LTail], RTail).

-spec subtract(RangeList, RangeList) -> RangeList
    when RangeList :: range_list().

%% Result = Right - Left.

subtract([], RangeList) ->
    RangeList;
subtract(_, []) ->
    [];
subtract([L = {LLo, _}|LTail], [R = {_, RHi}|RTail]) when LLo > RHi ->
    [R|subtract([L|LTail], RTail)];
subtract([{_, LHi}|LTail], [R = {RLo, _}|RTail]) when LHi < RLo ->
    subtract(LTail, [R|RTail]);
subtract([L = {LLo, LHi}|LTail], [{RLo, RHi}|RTail]) when LLo =< RLo, LHi >= RHi ->
    subtract([L|LTail], RTail);
subtract([{LLo, LHi}|LTail], [{RLo, RHi}|RTail]) when LLo =< RLo -> %% LHi < RHi
    subtract(LTail, [{LHi + 1, RHi}|RTail]);
subtract([L = {LLo, LHi}|LTail], [{RLo, RHi}|RTail]) when LHi >= RHi -> %% LLo > RLo
    [{RLo, LLo - 1}|subtract([L|LTail], RTail)];
subtract([{LLo, LHi}|LTail], [{RLo, RHi}|RTail]) -> %% LLo > RLo, LHi < RHi
    [{RLo, LLo - 1}|subtract(LTail, [{LHi + 1, RHi}|RTail])].

-spec subtract_range(range(), RangeList) -> RangeList
    when RangeList :: range_list().

subtract_range(Range, []) ->
    [Range];
subtract_range(Range = {_, Hi}, [{CurLo, _}|Tail]) when Hi < CurLo ->
    subtract_range(Range, Tail);
subtract_range({Lo, Hi}, [{CurLo, CurHi}|_]) when Lo >= CurLo, Hi =< CurHi ->
    [];
subtract_range({Lo, Hi}, [{CurLo, CurHi}|Tail]) when Lo >= CurLo ->
    subtract_range({CurHi + 1, Hi}, Tail);
subtract_range({Lo, Hi}, [{CurLo, CurHi}|_]) when Lo < CurLo, Hi =< CurHi ->
    [{Lo, CurLo - 1}];
subtract_range({Lo, Hi}, [{CurLo, CurHi}|Tail]) when Lo < CurLo, Hi > CurHi ->
    [{Lo, CurLo - 1}|subtract_range({CurHi + 1, Hi}, Tail)];
subtract_range(Range, [_|Tail]) ->
    subtract_range(Range, Tail).

test() ->

    %% from_list/to_list
    List = lists:seq(1,10) ++ [17] ++ lists:seq(20,40),
    RangeList = [{1,10},{17,17},{20,40}],
    RangeList = from_list(List),
    RangeList = from_list(to_list(RangeList)),
    List = to_list(RangeList),
    List = to_list(from_list(List)),

    %% merge
    F = fun merge/2,
    [{1,40}] = F([{1,1}], F([{8,9}], F([{21,29}], F([{5,7}], F([{2,4}], F([{30,40}], F([{10,20}], []))))))),
    [{1,40}] = merge([{7,15}], [{1,40}]),
    [{1,40}] = merge([{7,15}], [{1,6}, {16,40}]),
    [{1,40}] = merge([{7,15}], [{1,8}, {14,40}]),
    [{1,10}, {21,30}, {41,50}, {61,70}] = merge([{1,10}, {41,50}], [{21,30}, {61,70}]),

    %% subtract
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,40}]),
    [{19,19}, {26,26}] = subtract([{20,25}], [{19,19}, {26,26}]),
    [] = subtract([{20,25}], [{20,25}]),
    [] = subtract([{20,25}], [{20,20}, {25,25}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,19}, {23,23}, {25,40}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,20}, {23,23}, {25,40}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,21}, {23,23}, {25,40}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,21}, {23,23}, {24,40}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,21}, {23,23}, {25,40}]),
    [{1,19}, {26,40}] = subtract([{20,25}], [{1,21}, {23,23}, {26,40}]),
    [] = subtract([{1,1}], [{1,1}]),
    [{1,19}, {26,40}] = subtract([{20,23}, {25,25}], [{1,21}, {23,23}, {25,40}]),

    %% subtract_range
    [] = subtract_range({10,20}, [{1,40}]),
    [{41,50}] = subtract_range({10,50}, [{1,40}]),
    [{-1,0}, {41,50}] = subtract_range({-1,50}, [{1,40}]),
    [{0,0}, {41,42}, {47,50}] = subtract_range({0,50}, [{1,40},{43,46},{70,79}]),

    ok.
