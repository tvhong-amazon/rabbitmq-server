%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_global_counters).
-on_load(init/0).

-export([
         init/0,
         overview/0,
         prometheus_format/0,
         messages_received/1,
         messages_routed/1,
         messages_acknowledged/1,
         channel_get_ack/1,
         channel_get_empty/1,
         channel_messages_delivered_ack/1,
         channel_messages_delivered/1
       ]).


-define(MESSAGES_RECEIVED, 1).
-define(MESSAGES_ROUTED, 2).
-define(MESSAGES_ACKNOWLEDGED, 3).
-define(CHANNEL_GET_ACK, 4).
-define(CHANNEL_GET_EMPTY, 5).
-define(CHANNEL_MESSAGES_DELIVERED_ACK, 6).
-define(CHANNEL_MESSAGES_DELIVERED, 7).

-define(COUNTERS,
            [
                {
                    messages_received_total, ?MESSAGES_RECEIVED, counter,
                    "Total number of messages received from clients"
                },
                {
                    messages_acknowledged_total, ?MESSAGES_ACKNOWLEDGED, counter,
                    "Total number of message acknowledgements received from consumers"
                },
                {
                    messages_routed_total, ?MESSAGES_ROUTED, counter,
                    "Total number of messages routed to queues"
                },
                {
                    channel_get_ack_total, ?CHANNEL_GET_ACK, counter,
                    "Total number of messages fetched with basic.get in manual acknowledgement mode"
                },
                {
                    channel_get_empty_total, ?CHANNEL_GET_EMPTY, counter,
                    "Total number of times basic.get operations fetched no message"
                },
                {
                    channel_messages_delivered_ack_total, ?CHANNEL_MESSAGES_DELIVERED_ACK, counter,
                    "Total number of messages delivered to consumers in manual acknowledgement mode"
                },
                {
                    channel_messages_delivered_total, ?CHANNEL_MESSAGES_DELIVERED, counter,
                    "Total number of messages delivered to consumers in automatic acknowledgement mode"
                }
            ]).

init() ->
    %% Make this a test instead
    case lists:sort([ID || {_, ID, _, _} <- ?COUNTERS]) == lists:seq(1, length(?COUNTERS)) of
        false -> rabbit_log:critical("rabbit_global_counters indices are not a consequitive list of integers");
        true -> ok
    end,

    persistent_term:put(?MODULE,
                        counters:new(length(?COUNTERS), [write_concurrency])),
    ok.

overview() ->
    Counters = persistent_term:get(?MODULE),
    lists:foldl(
      fun ({Key, Index, _Type, _Description}, Acc) ->
              Acc#{Key => counters:get(Counters, Index)}
      end,
      #{},
      ?COUNTERS
     ).

prometheus_format() ->
    Counters = persistent_term:get(?MODULE),
    [{Key, counters:get(Counters, Index), Type, Help} ||
     {Key, Index, Type, Help} <- ?COUNTERS].

messages_received(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_RECEIVED, Num).

% formerly known as queue_messages_published_total
messages_routed(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_ROUTED, Num).

%% doesn't work when consuming messages through the API
messages_acknowledged(Num) ->
    counters:add(persistent_term:get(?MODULE), ?MESSAGES_ACKNOWLEDGED, Num).

channel_get_ack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?CHANNEL_GET_ACK, Num).

channel_get_empty(Num) ->
    counters:add(persistent_term:get(?MODULE), ?CHANNEL_GET_EMPTY, Num).

channel_messages_delivered_ack(Num) ->
    counters:add(persistent_term:get(?MODULE), ?CHANNEL_MESSAGES_DELIVERED_ACK, Num).

% TODO these are auto-ack only but the name doesn't reflect that
channel_messages_delivered(Num) ->
    counters:add(persistent_term:get(?MODULE), ?CHANNEL_MESSAGES_DELIVERED, Num).