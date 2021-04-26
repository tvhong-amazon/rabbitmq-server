%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_modern_queue_index).

-export([erase/1, init/3, reset_state/1, recover/6,
         terminate/3, delete_and_terminate/1,
         publish/7, deliver/2, ack/2, read/3]).

%% Recovery. Unlike other functions in this module, these
%% apply to all queues all at once.
-export([start/2, stop/1]).

%% rabbit_queue_index/rabbit_variable_queue-specific functions.
%% Implementation details from the queue index leaking into the
%% queue implementation itself.
-export([pre_publish/7, flush_pre_publish_cache/2,
         sync/1, needs_sync/1, flush/1,
         bounds/1, next_segment_boundary/1]).

-define(QUEUE_NAME_STUB_FILE, ".queue_name").
-define(SEGMENT_EXTENSION, ".midx").

-define(MAGIC, 16#524D5149). %% "RMQI"
-define(VERSION, 1).
-define(HEADER_SIZE, 64). %% bytes
-define(ENTRY_SIZE,  32). %% bytes

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/file.hrl").

%% Set to true to get an awful lot of debug logs.
-if(false).
-define(DEBUG(X,Y), logger:debug("~0p: " ++ X, [?FUNCTION_NAME|Y])).
-else.
-define(DEBUG(X,Y), _ = X, _ = Y, ok).
-endif.

-type seq_id() :: non_neg_integer().
%% @todo Use a shared seq_id() type in all relevant modules.

-type entry() :: {rabbit_types:msg_id(), seq_id(), rabbit_types:message_properties(), boolean(), boolean()}.

-record(mqistate, {
    %% Queue name (for the stub file).
    queue_name :: rabbit_amqqueue:name(),

    %% Queue index directory.
    dir :: file:filename(),

    %% seq_id() of the next message to be written
    %% in the index. Messages are written sequentially.
    %% Messages are always written to the index.
    write_marker = 0 :: seq_id(),

    %% Buffer of all write operations to be performed.
    %% When the buffer reaches a certain size, we reduce
    %% the buffer size by first checking if writing the
    %% delivers and acks gets us to a comfortable buffer
    %% size. If we do, write only the delivers and acks.
    %% If not, write everything. This allows the reader
    %% to continue reading from memory if it is fast
    %% enough to keep up with the producer.
    write_buffer = #{} :: #{seq_id() => entry() | deliver | ack},

    %% The number of entries in the write buffer that
    %% refer to an update to the file, rather than a
    %% full entry. Updates include deliver and ack.
    %% Used to determine if flushing only delivers/acks
    %% is enough to get the buffer to a comfortable size.
    write_buffer_updates = 0 :: non_neg_integer(),

    %% Messages waiting for publisher confirms. The
    %% publisher confirms will be sent when the message
    %% has been synced to disk (as an entry or an ack).
    %%
    %% The index does not call file:sync/1 by default.
    %% It only does when publisher confirms are used
    %% and there are outstanding unconfirmed messages.
    %% In that case the buffer is flushed to disk when
    %% the queue requests a sync (after a timeout).
    confirms = #{} :: #{seq_id() => rabbit_types:msg_id()},

    %% seq_id() of the next message to be read from disk.
    %% After the read_marker position there may be messages
    %% that are in transit (in the in_transit list) that
    %% will be skipped when setting the next read_marker
    %% position, or acked (see ack_marker/acks).
    %%
    %% When messages in in_transit are requeued we lower
    %% the read_marker position to the lowest value.
    read_marker = 0 :: seq_id(),

    %% seq_id() of messages that are in-transit. They
    %% could be in-transit because they have been read
    %% by the queue, or delivered to the consumer.
    in_transit = [] :: [seq_id()],

    %% seq_id() of the highest contiguous acked message.
    %% All messages before and including this seq_id()
    %% have been acked. We use this value both as an
    %% optimization and to know which segment files
    %% have been fully acked and can be deleted.
    %%
    %% In addition to the ack marker, messages in the
    %% index will also contain an ack byte flag that
    %% indicates whether that message was acked.
    ack_marker = undefined :: seq_id() | undefined,

    %% seq_id() of messages that have been acked
    %% and are higher than the ack_marker. Messages
    %% are only listed here when there are unacked
    %% messages before them.
    acks = [] :: [seq_id()],

    %% File descriptors.
    %% @todo The current implementation does not limit the number of FDs open.
    %%       This is most likely not desirable... Perhaps we can have a limit
    %%       per queue index instead of relying on file_handle_cache.
    fds = #{} :: #{non_neg_integer() => file:fd()},

    %% This fun must be called when messages that expect
    %% confirms have either an ack or their entry
    %% written to disk and file:sync/1 has been called.
    on_sync :: on_sync_fun()
}).

-type mqistate() :: #mqistate{}.
-export_type([mqistate/0]).

%% Types copied from rabbit_queue_index.

-type on_sync_fun() :: fun ((gb_sets:set()) -> ok).
-type contains_predicate() :: fun ((rabbit_types:msg_id()) -> boolean()).
-type shutdown_terms() :: list() | 'non_clean_shutdown'.

%% ----

-spec erase(rabbit_amqqueue:name()) -> 'ok'.

erase(#resource{ virtual_host = VHost } = Name) ->
    ?DEBUG("~0p", [Name]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    erase_index_dir(Dir).

-spec init(rabbit_amqqueue:name(),
                 on_sync_fun(), on_sync_fun()) -> mqistate().

%% We do not embed messages and as a result never need the OnSyncMsgFun.

init(#resource{ virtual_host = VHost } = Name, OnSyncFun, _OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p", [Name, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    false = rabbit_file:is_file(Dir), %% is_file == is file or dir
    init1(Name, Dir, OnSyncFun).

init1(Name, Dir, OnSyncFun) ->
    ensure_queue_name_stub_file(Name, Dir),
    ensure_segment_file(new, #mqistate{
        queue_name = Name,
        dir = Dir,
        on_sync = OnSyncFun
    }).

ensure_queue_name_stub_file(#resource{virtual_host = VHost, name = QName}, Dir) ->
    QueueNameFile = filename:join(Dir, ?QUEUE_NAME_STUB_FILE),
    ok = filelib:ensure_dir(QueueNameFile),
    ok = file:write_file(QueueNameFile, <<"VHOST: ", VHost/binary, "\n",
                                          "QUEUE: ", QName/binary, "\n">>).

ensure_segment_file(Reason, State = #mqistate{ write_marker = WriteMarker,
                                               fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    Segment = WriteMarker div SegmentEntryCount,
    %% The file might already be opened during recovery. Don't open it again.
    Fd = case {Reason, OpenFds} of
        {recover, #{Segment := Fd0}} ->
            Fd0;
        _ ->
            false = maps:is_key(Segment, OpenFds), %% assert
            {ok, Fd0} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            Fd0
    end,
    %% When we are recovering from a shutdown we may need to check
    %% whether the file already has content. If it does, we do
    %% nothing. Otherwise, as well as in the normal case, we
    %% pre-allocate the file size and write in the file header.
    IsNewFile = case Reason of
        new ->
            true;
        recover ->
            {ok, #file_info{ size = Size }} = file:read_file_info(Fd),
            Size =:= 0
    end,
    case IsNewFile of
        true ->
            %% We preallocate space for the file when possible.
            %% We don't worry about failure here because an error
            %% is returned when the system doesn't support this feature.
            %% The index will simply be less efficient in that case.
            _ = file:allocate(Fd, 0, ?HEADER_SIZE + SegmentEntryCount * ?ENTRY_SIZE),
            %% We then write the segment file header. It contains
            %% some useful info and some reserved bytes for future use.
            %% We currently do not make use of this information. It is
            %% only there for forward compatibility purposes (for example
            %% to support an index with mixed segment_entry_count() values).
            FromSeqId = (WriteMarker div SegmentEntryCount) * SegmentEntryCount,
            ToSeqId = FromSeqId + SegmentEntryCount,
            ok = file:write(Fd, << ?MAGIC:32,
                                   ?VERSION:8,
                                   FromSeqId:64/unsigned,
                                   ToSeqId:64/unsigned,
                                   0:344 >>);
        false ->
            ok
    end,
    %% Keep the file open.
    State#mqistate{ fds = OpenFds#{Segment => Fd} }.

-spec reset_state(State) -> State when State::mqistate().

reset_state(State = #mqistate{ queue_name     = Name,
                               dir            = Dir,
                               on_sync        = OnSyncFun }) ->
    ?DEBUG("~0p", [State]),
    delete_and_terminate(State),
    init1(Name, Dir, OnSyncFun).

-spec recover(rabbit_amqqueue:name(), shutdown_terms(), boolean(),
                    contains_predicate(),
                    on_sync_fun(), on_sync_fun()) ->
                        {'undefined' | non_neg_integer(),
                         'undefined' | non_neg_integer(), mqistate()}.

-define(RECOVER_COUNT, 1).
-define(RECOVER_BYTES, 2).

recover(#resource{ virtual_host = VHost } = Name, Terms, IsMsgStoreClean,
        ContainsCheckFun, OnSyncFun, _OnSyncMsgFun) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p", [Name, Terms, IsMsgStoreClean, ContainsCheckFun, OnSyncFun]),
    VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
    Dir = queue_dir(VHostDir, Name),
    State0 = init1(Name, Dir, OnSyncFun),
    %% We go over all segments if either the index or the
    %% message store has/had to recover. Otherwise we just
    %% take our state from Terms.
    IsIndexClean = Terms =/= non_clean_shutdown,
    {Count, Bytes, State2} = case IsIndexClean andalso IsMsgStoreClean of
        true ->
            {WriteMarker0, AckMarker0, Acks0} = proplists:get_value(mqi_state, Terms, {0, undefined, []}),
            %% The queue has stored the count/bytes values inside
            %% Terms so we don't need to provide them again.
            %% However I can see this being a problem if
            %% there's been corruption on the disk. But
            %% a better fix would be to rework the way
            %% rabbit_variable_queue works.
            {undefined,
             undefined,
             State0#mqistate{ write_marker = WriteMarker0,
                              ack_marker = AckMarker0,
                              acks = Acks0 }};
        false ->
            CountersRef = counters:new(2, []),
            State1 = recover_segments(State0, ContainsCheckFun, CountersRef),
            {counters:get(CountersRef, ?RECOVER_COUNT),
             counters:get(CountersRef, ?RECOVER_BYTES),
             State1}
    end,
    %% We double check the ack marker because when the message store has
    %% recovered we might have marked additional messages as acked as
    %% part of the recovery process.
    State3 = recover_maybe_advance_ack_marker(State2),
    %% We set the read marker to the first message after the ack marker.
    State = recover_read_marker(State3),
    %% We can now ensure the current segment file is in a good state and return.
    {Count, Bytes, ensure_segment_file(recover, State)}.

recover_segments(State0 = #mqistate { dir = Dir }, ContainsCheckFun, CountersRef) ->
    %% Figure out which files exist. Sort them in segment order.
    SegmentFiles = rabbit_file:wildcard(".*\\" ++ ?SEGMENT_EXTENSION, Dir),
    Segments = lists:sort([
        list_to_integer(filename:basename(F, ?SEGMENT_EXTENSION))
    || F <- SegmentFiles]),
    %% @todo We want to figure out if some files are missing (there are holes)
    %%       and if so, we want to create new fully acked files and add those
    %%       messages to the acks list.
    %%
    %% @todo We may want to check that the file sizes are correct before attempting
    %%       to parse them, and to correct the file sizes. Correcting file sizes
    %%       may create holes within segment files though if we only file:allocate.
    %%       Not sure what should be done about that one.
    case Segments of
        %% No segments found. Keep default values.
        [] ->
            State0;
        %% Look for markers and acks in the files we've found.
        [FirstSegment|Tail] ->
            {Fd, State} = get_fd_for_segment(FirstSegment, State0),
            SegmentEntryCount = segment_entry_count(),
            FirstSeqId = FirstSegment * SegmentEntryCount,
            recover_ack_marker(State, ContainsCheckFun, CountersRef,
                               Fd, FirstSegment, SegmentEntryCount,
                               FirstSeqId, Tail)
    end.

recover_ack_marker(State, ContainsCheckFun, CountersRef,
                   Fd, FdSegment, SegmentEntryCount, SeqId, Segments) ->
    case locate(SeqId, SegmentEntryCount) of
        %% The SeqId is in the current segment file.
        {FdSegment, Offset} ->
            case file:pread(Fd, Offset, 1) of
                %% We found an ack, continue.
                {ok, <<2>>} ->
                    recover_ack_marker(State, ContainsCheckFun, CountersRef,
                                       Fd, FdSegment, SegmentEntryCount,
                                       SeqId + 1, Segments);
                %% We found a non-ack entry! The previous SeqId is our ack_marker.
                %% We now need to figure out where the write_marker ends.
                {ok, <<1>>} ->
                    AckMarker = case SeqId of
                        0 -> undefined;
                        _ -> SeqId - 1
                    end,
                    recover_write_marker(State#mqistate{ ack_marker = AckMarker },
                                         ContainsCheckFun, CountersRef,
                                         Fd, FdSegment, SegmentEntryCount,
                                         SeqId + 1, Segments, []);
                %% We found a non-entry. Everything was acked.
                %%
                %% @todo We may want to find out whether this is a hole in the file
                %%       or the real end of the segments. If this is a hole, we could
                %%       repair it automatically.
                {ok, <<0>>} ->
                    AckMarker = case SeqId of
                        0 -> undefined;
                        _ -> SeqId - 1
                    end,
                    State#mqistate{ write_marker = SeqId,
                                    ack_marker = AckMarker }
            end;
        %% The SeqId is in another segment file. The current file has been
        %% fully acked. Delete this file and open the next segment file if any.
        %% If there are no more files this means that everything was acked and
        %% that we can start again with default values.
        {_, _} ->
            EofState = delete_segment(FdSegment, State),
            case Segments of
                [] ->
                    EofState;
                [NextSegment|Tail] ->
                    {NextFd, NextState} = get_fd_for_segment(NextSegment, EofState),
                    NextSeqId = NextSegment * SegmentEntryCount, %% Might not be =:= SeqId if files are not contiguous.
                    recover_ack_marker(NextState, ContainsCheckFun, CountersRef,
                                       NextFd, NextSegment, SegmentEntryCount,
                                       NextSeqId, Tail)
            end
    end.

%% This function does two things: it recovers the write_marker,
%% and it ensures that non-acked entries found have a corresponding
%% message in the message store.

recover_write_marker(State, ContainsCheckFun, CountersRef,
                     Fd, FdSegment, SegmentEntryCount, SeqId, Segments, Acks) ->
    case locate(SeqId, SegmentEntryCount) of
        %% The SeqId is in the current segment file.
        {FdSegment, Offset} ->
            case file:pread(Fd, Offset, 24) of
                %% We found an ack, add it to the acks list.
                {ok, <<2,_/bits>>} ->
                    recover_write_marker(State, ContainsCheckFun, CountersRef,
                        Fd, FdSegment, SegmentEntryCount,
                        SeqId + 1, Segments, [SeqId|Acks]);
                %% We found a non-ack entry. Check that the corresponding message
                %% exists in the message store. If not, mark this message as acked.
                %%
                %% @todo rabbit_queue_index marks all messages as delivered
                %%       when the shutdown was not clean. Should we do the same?
                {ok, <<1,_:24,Id:16/binary,Size:32/unsigned>>} ->
                    NextAcks = case ContainsCheckFun(Id) of
                        %% Message is in the store.
                        true ->
                            counters:add(CountersRef, ?RECOVER_COUNT, 1),
                            counters:add(CountersRef, ?RECOVER_BYTES, Size),
                            Acks;
                        %% Message is not in the store. Mark it as acked.
                        false ->
                            file:pwrite(Fd, Offset, <<2>>),
                            [SeqId|Acks]
                    end,
                    recover_write_marker(State, ContainsCheckFun, CountersRef,
                        Fd, FdSegment, SegmentEntryCount,
                        SeqId + 1, Segments, NextAcks);
                %% We found a non-entry. The SeqId is our write_marker.
                %%
                %% @todo We also want to find out whether this is a hole in the file
                %%       or the real end of the segments. If this is a hole, we could
                %%       repair it automatically.
                {ok, <<0,_/bits>>} when Segments =:= [] ->
                    State#mqistate{ write_marker = SeqId,
                                    acks = Acks }
            end;
        %% The SeqId is in another segment file.
        {_, _} ->
            case Segments of
                %% This was the last segment file. The SeqId is our
                %% write_marker.
                [] ->
                    State#mqistate{ write_marker = SeqId,
                                    acks = Acks };
                %% Continue reading.
                [NextSegment|Tail] ->
                    {NextFd, NextState} = get_fd_for_segment(NextSegment, State),
                    NextSeqId = NextSegment * SegmentEntryCount, %% Might not be =:= SeqId if files are not contiguous.
                    recover_write_marker(NextState, ContainsCheckFun, CountersRef,
                        NextFd, NextSegment, SegmentEntryCount,
                        NextSeqId, Tail, Acks)
            end
    end.

recover_maybe_advance_ack_marker(State = #mqistate{ ack_marker = AckMarker0,
                                                    acks = Acks0 }) ->
    NextAckMarker = case AckMarker0 of
        undefined -> 0;
        _ -> AckMarker0 + 1
    end,
    case lists:member(NextAckMarker, Acks0) of
        %% The ack_marker must be advanced because we found
        %% the next seq_id() in the acks list.
        true ->
            AckMarker = highest_continuous_seq_id(Acks0),
            Acks = lists:dropwhile(
                fun(AckSeqId) -> AckSeqId =< AckMarker end,
                Acks0),
            State#mqistate{ ack_marker = AckMarker,
                            acks = Acks };
        %% The ack_marker is correct.
        false ->
            State
    end.

recover_read_marker(State = #mqistate{ ack_marker = undefined }) ->
    State; %% Default read_marker is 0.
recover_read_marker(State = #mqistate{ ack_marker = AckMarker }) ->
    State#mqistate{ read_marker = AckMarker + 1 }.

-spec terminate(rabbit_types:vhost(), [any()], State) -> State when State::mqistate().

terminate(VHost, Terms, State0 = #mqistate { dir = Dir,
                                             write_marker = WriteMarker,
                                             ack_marker = AckMarker,
                                             acks = Acks,
                                             fds = OpenFds }) ->
    ?DEBUG("~0p ~0p ~0p", [VHost, Terms, State0]),
    %% Flush the buffer.
    State = flush_buffer(State0, full),
    %% Fsync and close all FDs.
    _ = maps:map(fun(_, Fd) ->
        ok = file:sync(Fd),
        ok = file:close(Fd)
    end, OpenFds),
    %% Write recovery terms for faster recovery.
    Term = {WriteMarker, AckMarker, Acks},
    rabbit_recovery_terms:store(VHost, filename:basename(Dir),
                                [{mqi_state, Term} | Terms]),
    State#mqistate{ fds = #{} }.

-spec delete_and_terminate(State) -> State when State::mqistate().

delete_and_terminate(State = #mqistate { dir = Dir,
                                         fds = OpenFds }) ->
    ?DEBUG("~0p", [State]),
    %% Close all FDs.
    _ = maps:map(fun(_, Fd) ->
        ok = file:close(Fd)
    end, OpenFds),
    %% Erase the data on disk.
    ok = erase_index_dir(Dir),
    State#mqistate{ fds = #{} }.

-spec publish(rabbit_types:msg_id(), seq_id(),
                    rabbit_types:message_properties(), boolean(), boolean(),
                    non_neg_integer(), State) -> State when State::mqistate().

%% @todo Because we always persist to the msg_store, the MsgOrId argument
%%       here is always a binary, never a record.

publish(MsgOrId, SeqId, Props, IsPersistent, _IsDelivered, TargetRamCount,
        State0 = #mqistate { write_marker = WriteMarker,
                             read_marker = ReadMarker0,
                             in_transit = InTransit })
        when SeqId < WriteMarker ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Props, IsPersistent, TargetRamCount, State0]),
    %% We have already written this message on disk. We do not need
    %% to write it again. We may instead remove it from the in_transit list.
    %% @todo Confirm that this is indeed the behavior we should have.
    %% @todo It would be better to have a separate function for this...
    ReadMarker = if
        SeqId < ReadMarker0 -> SeqId;
        true -> ReadMarker0
    end,
    State = State0#mqistate{ read_marker = ReadMarker,
                             in_transit = InTransit -- [SeqId] },
    %% @todo When messages are "put back" into the index, we may
    %%       not want to write the deliver flag again. This is a
    %%       waste of resources if the message already has the flag.
    %% @todo To be honest I don't think we need to... We should
    %%       already have gotten a deliver/2 call for it.
%    case IsDelivered of
%        true -> deliver([SeqId], State);
%        false -> State
%    end.
    State;

publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount,
        State0 = #mqistate { write_marker = WriteMarker,
                             write_buffer = WriteBuffer0 }) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Props, IsPersistent, TargetRamCount, State0]),
    %% When the message was delivered we need to add it to
    %% the in_transit list and possibly advance the read_marker.
    State1 = case IsDelivered of
        true -> mark_in_transit([SeqId], State0);
        false -> State0
    end,
    %% We can then add the entry to the write buffer.
    Id = case MsgOrId of
        #basic_message{id = Id0} -> Id0;
        Id0 when is_binary(Id0) -> Id0
    end,
    WriteBuffer = WriteBuffer0#{SeqId => {Id, SeqId, Props, IsPersistent, IsDelivered}},
    NextWriteMarker = WriteMarker + 1,
    State2 = State1#mqistate{ write_marker = NextWriteMarker,
                              write_buffer = WriteBuffer },
    %% When the write_marker ends up on a new segment we must
    %% prepare the new segment file even if we don't flush
    %% the buffer and write to that file yet.
    SegmentEntryCount = segment_entry_count(),
    ThisSegment = WriteMarker div SegmentEntryCount,
    NextSegment = NextWriteMarker div SegmentEntryCount,
    State3 = case ThisSegment =:= NextSegment of
        true -> State2;
        false -> ensure_segment_file(new, State2)
    end,
    %% When publisher confirms have been requested for this
    %% message we mark the message as unconfirmed.
    State = maybe_mark_unconfirmed(MsgOrId, SeqId, Props, State3),
    maybe_flush_buffer(State).

maybe_mark_unconfirmed(MsgId, SeqId, #message_properties{ needs_confirming = true },
        State = #mqistate { confirms = Confirms }) ->
    State#mqistate{ confirms = Confirms#{SeqId => MsgId} };
maybe_mark_unconfirmed(_, _, _, State) ->
    State.

%% @todo Perhaps make the two limits configurable.
maybe_flush_buffer(State = #mqistate { write_buffer = WriteBuffer })
        when map_size(WriteBuffer) < 2000 ->
    State;
maybe_flush_buffer(State = #mqistate { write_buffer_updates = NumUpdates }) ->
    FlushType = case NumUpdates >= 100 of
        true -> updates;
        false -> full
    end,
    flush_buffer(State, FlushType).

%% When there are less than 100 entries, we only write updates
%% (deliver | ack) and get the buffer back to a comfortable level.

flush_buffer(State0 = #mqistate { write_buffer = WriteBuffer0 },
             FlushType) ->
    SegmentEntryCount = segment_entry_count(),
    %% First we prepare the writes sorted by segment.
    {Writes, WriteBuffer} = maps:fold(fun
        (SeqId, ack, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, <<2>>, +0, WritesAcc),
             BufferAcc};
        (SeqId, deliver, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, <<1>>, +1, WritesAcc),
             BufferAcc};
        (SeqId, Entry, {WritesAcc, BufferAcc}) when FlushType =:= updates ->
            {WritesAcc,
             BufferAcc#{SeqId => Entry}};
        %% Otherwise we write the entire buffer.
        (SeqId, Entry, {WritesAcc, BufferAcc}) ->
            {acc_write(SeqId, SegmentEntryCount, build_entry(Entry), +0, WritesAcc),
             BufferAcc}
    end, {#{}, #{}}, WriteBuffer0),
    %% Then we do the writes for each segment.
    State = maps:fold(fun(Segment, LocBytes, FoldState0) ->
        {Fd, FoldState} = get_fd_for_segment(Segment, FoldState0),
        ok = file:pwrite(Fd, LocBytes),
        FoldState
    end, State0, Writes),
    %% Finally we update the state.
    State#mqistate{ write_buffer = WriteBuffer,
                    write_buffer_updates = 0 }.

acc_write(SeqId, SegmentEntryCount, Bytes, EntryOffset, WritesAcc) ->
    {Segment, Offset} = locate(SeqId, SegmentEntryCount),
    LocBytesAcc = maps:get(Segment, WritesAcc, []),
    WritesAcc#{Segment => [{Offset + EntryOffset, Bytes}|LocBytesAcc]}.

build_entry({Id, _SeqId, Props, IsPersistent, IsDelivered}) ->
    IsDeliveredFlag = case IsDelivered of
        true -> 1;
        false -> 0
    end,
    Flags = case IsPersistent of
        true -> 1;
        false -> 0
    end,
    #message_properties{ expiry = Expiry0, size = Size } = Props,
    Expiry = case Expiry0 of
        undefined -> 0;
        _ -> Expiry0
    end,
    << 1:8,                   %% Status. 0 = no entry, 1 = entry exists, 2 = entry acked
       IsDeliveredFlag:8,     %% Deliver.
       Flags:8,               %% IsPersistent flag (least significant bit).
       0:8,                   %% Reserved. Makes entries 32B in size to match page alignment on disk.
       Id:16/binary,          %% Message store ID.
       Size:32/unsigned,      %% Message payload size.
       Expiry:64/unsigned >>. %% Expiration time.

get_fd_for_segment(Segment, State = #mqistate{ fds = OpenFds }) ->
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, State#mqistate{ fds = OpenFds#{ Segment => Fd }}}
    end.

%% When marking delivers we may need to update the file(s) on disk.

-spec deliver([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
deliver([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
deliver(SeqIds, State0 = #mqistate { write_buffer = WriteBuffer0,
                                     write_buffer_updates = NumUpdates0 }) ->
    ?DEBUG("~0p ~0p", [SeqIds, State0]),
    %% Update the in_transit list. We also update the read_marker if necessary.
    %% @todo Is this still necessary with publish taking care of delivers?
    %%       This was necessary before because publish and deliver were called one after the other.
    State = mark_in_transit(SeqIds, State0),
    %% Add delivers to the write buffer if necessary.
    {WriteBuffer, NumUpdates} = lists:foldl(fun
        (SeqId, {FoldBuffer, FoldUpdates}) ->
            case FoldBuffer of
                %% Update the IsDelivered flag in the entry if any.
                #{SeqId := {Id, SeqId, Props, IsPersistent, false}} ->
                    {FoldBuffer#{SeqId => {Id, SeqId, Props, IsPersistent, true}},
                     FoldUpdates};
                %% If any other write exists, do nothing.
                #{SeqId := _} ->
                    {FoldBuffer,
                     FoldUpdates};
                %% Otherwise mark for delivery and increase the updates count.
                _ ->
                    {FoldBuffer#{SeqId => deliver},
                     FoldUpdates + 1}
            end
    end, {WriteBuffer0, NumUpdates0}, SeqIds),
    maybe_flush_buffer(State#mqistate{ write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates }).

mark_in_transit(SeqIds, State = #mqistate{ read_marker = ReadMarker0,
                                           in_transit = InTransit0 }) ->
    %% @todo Would be good to avoid this lists:sort/1 call.
    ReadMarker = maybe_advance_read_marker(ReadMarker0, lists:sort(SeqIds)),
    %% @todo We probably shouldn't do this lists:usort as well. Hacky quick fix.
    InTransit = lists:usort(SeqIds ++ InTransit0),
    State#mqistate{ read_marker = ReadMarker, in_transit = InTransit }.

maybe_advance_read_marker(ReadMarker, [ReadMarker]) ->
    ReadMarker + 1;
maybe_advance_read_marker(ReadMarker, [ReadMarker|Tail]) ->
    highest_continuous_seq_id(Tail) + 1;
maybe_advance_read_marker(ReadMarker, _) ->
    ReadMarker.

%% When marking acks we need to update the file(s) on disk
%% as well as update ack_marker and/or acks in the state.
%% When a file has been fully acked we may also close its
%% open FD if any and delete it.

-spec ack([seq_id()], State) -> State when State::mqistate().

%% The rabbit_variable_queue module may call this function
%% with an empty list. Do nothing.
ack([], State) ->
    ?DEBUG("[] ~0p", [State]),
    State;
ack(SeqIds0, State0 = #mqistate{ write_buffer = WriteBuffer0,
                                 write_buffer_updates = NumUpdates0,
                                 in_transit = InTransit0,
                                 ack_marker = AckMarkerBefore }) ->
    ?DEBUG("~0p ~0p", [SeqIds0, State0]),
    %% We start by removing all the messages that were in transit.
    %% @todo Also remove everything below the ack_marker? Not sure why it's not all in the SeqIds...
    %%       No. If there are things left in in_transit, that's because they were added to the list twice.
    InTransit = InTransit0 -- SeqIds0,
    %% We continue by updating the ack state information. We then
    %% use this information to determine if we can delete
    %% segment files on disk.
    State1 = update_ack_state(SeqIds0, State0),
    {HighestSegmentDeleted, State} = maybe_delete_segments(AckMarkerBefore, State1),
    %% When there were deleted files, we remove the seq_id()s that
    %% belong to deleted files from the acked list, as well as any
    %% write instructions in the buffer targeting those files.
    {SeqIds, WriteBuffer2, NumUpdates2} = case HighestSegmentDeleted of
        undefined ->
            {SeqIds0, WriteBuffer0, NumUpdates0};
        _ ->
            %% All seq_id()s below this belong to a segment that has been
            %% fully acked and is no longer tracked by the index.
            LowestSeqId = (1 + HighestSegmentDeleted) * segment_entry_count(),
            SeqIds1 = lists:filter(fun(FilterSeqId) ->
                FilterSeqId >= LowestSeqId
            end, SeqIds0),
            {WriteBuffer1, NumUpdates1} = maps:fold(fun
                (FoldSeqId, Write, {FoldBuffer, FoldUpdates}) when FoldSeqId < LowestSeqId ->
                    Num = case Write of
                        deliver -> 1;
                        ack -> 1;
                        _ -> 0
                    end,
                    {FoldBuffer, FoldUpdates - Num};
                (SeqId, Write, {FoldBuffer, FoldUpdates}) ->
                    {FoldBuffer#{SeqId => Write}, FoldUpdates}
            end, {#{}, NumUpdates0}, WriteBuffer0),
            {SeqIds1, WriteBuffer1, NumUpdates1}
    end,
    %% Finally we add any remaining acks to the write buffer.
    {WriteBuffer, NumUpdates} = lists:foldl(fun
        (SeqId, {FoldBuffer, FoldUpdates}) ->
            case FoldBuffer of
                %% Ack if the entry was already marked for delivery or acked.
                %% @todo Is this possible to have ack here? Shouldn't this be an error?
                #{SeqId := Write} when Write =:= deliver; Write =:= ack ->
                    {FoldBuffer#{SeqId => ack},
                     FoldUpdates};
                %% Otherwise unconditionally ack. We replace the entry if any.
                _ ->
                    {FoldBuffer#{SeqId => ack},
                     FoldUpdates + 1}
            end
    end, {WriteBuffer2, NumUpdates2}, SeqIds),
    maybe_flush_buffer(State#mqistate{ write_buffer = WriteBuffer,
                                       write_buffer_updates = NumUpdates,
                                       in_transit = InTransit }).

update_ack_state(SeqIds, State = #mqistate{ ack_marker = undefined, acks = Acks0 }) ->
    Acks = lists:sort(SeqIds ++ Acks0),
    %% We must special case when there is no ack_marker in the state.
    %% The message with seq_id() 0 has not been acked before now.
    case Acks of
        [0|_] ->
            update_markers(Acks, State);
        _ ->
            State#mqistate{ acks = Acks }
    end;
update_ack_state(SeqIds, State = #mqistate{ ack_marker = AckMarker, acks = Acks0 }) ->
    Acks = lists:sort(SeqIds ++ Acks0),
    case Acks of
        %% When SeqId is the message after the marker, we update the marker
        %% and potentially remove additional SeqIds from the acks list. The
        %% new ack marker becomes the largest continuous seq_id() found in
        %% Acks.
        [SeqId|_] when SeqId =:= AckMarker + 1 ->
            update_markers(Acks, State);
        _ ->
            State#mqistate{ acks = Acks }
    end.

update_markers(Acks, State = #mqistate { read_marker = ReadMarker0 }) ->
    AckMarker = highest_continuous_seq_id(Acks),
    RemainingAcks = lists:dropwhile(
        fun(AckSeqId) -> AckSeqId =< AckMarker end,
        Acks),
    %% Advance the read_marker if the ack_marker got past it.
    %% @todo This is only necessary if acks can arrive after a
    %%       dirty shutdown I believe.
    ReadMarker = if
        AckMarker >= ReadMarker0 -> AckMarker + 1;
        true -> ReadMarker0
    end,
    State#mqistate{ read_marker = ReadMarker,
                    ack_marker = AckMarker,
                    acks = RemainingAcks }.

maybe_delete_segments(AckMarkerBefore, State = #mqistate{ ack_marker = AckMarkerAfter }) ->
    SegmentEntryCount = segment_entry_count(),
    %% We add one because the ack_marker points to the highest
    %% continuous acked message, and we want to know which
    %% segment is after that, so that we immediately remove
    %% a fully-acked file when ack_marker points to the last
    %% entry in the file.
    SegmentBefore = case AckMarkerBefore of
        undefined -> 0;
        _ -> (1 + AckMarkerBefore) div SegmentEntryCount
    end,
    SegmentAfter = (1 + AckMarkerAfter) div SegmentEntryCount,
    if
        %% When the marker still points to the same segment,
        %% do nothing.
        SegmentBefore =:= SegmentAfter ->
            {undefined, State};
        %% When the marker points to a different segment,
        %% delete fully acked segment files.
        true ->
            delete_segments(SegmentBefore, SegmentAfter - 1, State)
    end.

delete_segments(Segment, Segment, State) ->
    {Segment, delete_segment(Segment, State)};
delete_segments(Segment, LastSegment, State) ->
    delete_segments(Segment + 1, LastSegment,
        delete_segment(Segment, State)).

delete_segment(Segment, State0 = #mqistate{ fds = OpenFds0 }) ->
    %% We close the open fd if any.
    State = case maps:take(Segment, OpenFds0) of
        {Fd, OpenFds} ->
            ok = file:close(Fd),
            State0#mqistate{ fds = OpenFds };
        error ->
            State0
    end,
    %% Then we can delete the segment file.
    ok = file:delete(segment_file(Segment, State)),
    State.

%% A better interface for read/3 would be to request a maximum
%% of N messages, rather than first call next_segment_boundary/3
%% and then read from S1 to S2. This function could then return
%% either N messages or less depending on the current state.

-spec read(seq_id(), seq_id(), State) ->
                     {[entry()], State}
                     when State::mqistate().

%% From is inclusive, To is exclusive.
%% @todo This function is incompatible with the rabbit_queue_index function.
%%       In our case we may send messages >= ToSeqId. As a result the
%%       rabbit_variable_queue module may need to be accomodated.

read(FromSeqId, FromSeqId, State) ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, FromSeqId, State]),
    {[], State};
read(FromSeqId, ToSeqId, State) ->
    ?DEBUG("~0p ~0p ~0p", [FromSeqId, ToSeqId, State]),
    %% @todo Temporary read_marker here. Yeah we really ought to follow the same interface... Sigh.
    read(ToSeqId - FromSeqId, State#mqistate{ read_marker = FromSeqId }).

%% There might be messages after the read marker that were acked.
%% We need to skip those messages when we attempt to read them.

read(_, State = #mqistate{ write_marker = WriteMarker,
                           read_marker = ReadMarker })
        when WriteMarker =:= ReadMarker ->
    {[], State};
read(Num, State0 = #mqistate{ write_marker = WriteMarker,
                              write_buffer = WriteBuffer,
                              read_marker = ReadMarker,
                              in_transit = InTransit0,
                              acks = Acks  }) ->
    %% The first message is always readable. It cannot be acked
    %% because if it was, the ack_marker would advance, and the
    %% read_marker would advance as well. It cannot be in_transit,
    %% because if it was, the read_marker would advance to reflect
    %% that as well.
    {NextReadMarker, SeqIdsToRead0} = prepare_read(Num - 1, WriteMarker,
                                                   ReadMarker + 1, InTransit0,
                                                   Acks, []),
    SeqIdsToRead = [ReadMarker|SeqIdsToRead0],
    %% We first try to read from the write buffer what we can,
    %% then we read the rest from disk.
    {Reads0, SeqIdsOnDisk} = read_from_buffer(SeqIdsToRead,
                                              WriteBuffer,
                                              [], []),
    {Reads, State} = read_from_disk(SeqIdsOnDisk,
                                    State0,
                                    Reads0),
    %% We add the messages that have been read to the in_transit list.
    InTransit = lists:foldl(fun({_, SeqId, _, _, _}, Acc) -> [SeqId|Acc] end,
                            InTransit0, Reads),
    {Reads, State#mqistate{ read_marker = NextReadMarker,
                            in_transit = InTransit }}.

%% Return when we have found as many messages as requested.
prepare_read(0, _, ReadMarker, _, _, Acc) ->
    {ReadMarker, lists:reverse(Acc)};
%% Return when we have reached the end of messages in the index.
prepare_read(_, WriteMarker, ReadMarker, _, _, Acc)
        when WriteMarker =:= ReadMarker ->
    {ReadMarker, lists:reverse(Acc)};
%% For each seq_id() we may read, we check whether they are in
%% the in_transit list or in the acks list. If they are, we should
%% not read them. Otherwise we add them to the list of messages
%% that we want to read.
prepare_read(Num, WriteMarker, ReadMarker, InTransit, Acks, Acc) ->
    %% We arbitrarily look into the in_transit list first.
    %% It is unknown whether looking into the acks list
    %% first would provide better performance.
    Skip = lists:member(ReadMarker, lists:usort(InTransit))
        orelse lists:member(ReadMarker, Acks),
    case Skip of
        true ->
            prepare_read(Num, WriteMarker, ReadMarker + 1, InTransit, Acks, Acc);
        false ->
            prepare_read(Num - 1, WriteMarker, ReadMarker + 1, InTransit, Acks, [ReadMarker|Acc])
    end.

read_from_buffer([], _, SeqIdsOnDisk, Reads) ->
    {Reads, SeqIdsOnDisk};
read_from_buffer([SeqId|Tail], WriteBuffer, SeqIdsOnDisk, Reads) ->
    case WriteBuffer of
        #{SeqId := ack} ->
            read_from_buffer(Tail, WriteBuffer, SeqIdsOnDisk, Reads);
        #{SeqId := Entry} when is_tuple(Entry) ->
            read_from_buffer(Tail, WriteBuffer, SeqIdsOnDisk, [Entry|Reads]);
        _ ->
            read_from_buffer(Tail, WriteBuffer, [SeqId|SeqIdsOnDisk], Reads)
    end.

%% We try to minimize the number of file:read calls when reading from
%% the disk. We find the number of continuous messages, read them all
%% at once, and then repeat the loop.

read_from_disk([], State, Acc) ->
    {Acc, State};
read_from_disk(SeqIdsToRead0, State0, Acc0) ->
    FirstSeqId = hd(SeqIdsToRead0),
    %% We get the highest continuous seq_id() from the same segment file.
    %% If there are more continuous entries we will read them on the
    %% next loop.
    {LastSeqId, SeqIdsToRead} = highest_continuous_seq_id(SeqIdsToRead0,
                                                          next_segment_boundary(FirstSeqId)),
    ReadSize = (LastSeqId - FirstSeqId + 1) * ?ENTRY_SIZE,
    {Fd, OffsetForSeqId, State} = get_fd(FirstSeqId, State0),
    %% @todo Use file:pread/2?
    {ok, EntriesBin} = file:pread(Fd, OffsetForSeqId, ReadSize),
    %% We cons new entries into the Acc and only reverse it when we
    %% are completely done reading new entries.
    Acc = parse_entries(EntriesBin, FirstSeqId, Acc0),
    read_from_disk(SeqIdsToRead, State, Acc).

get_fd(SeqId, State = #mqistate{ fds = OpenFds }) ->
    SegmentEntryCount = segment_entry_count(),
    {Segment, Offset} = locate(SeqId, SegmentEntryCount),
    case OpenFds of
        #{ Segment := Fd } ->
            {Fd, Offset, State};
        _ ->
            {ok, Fd} = file:open(segment_file(Segment, State), [read, write, raw, binary]),
            {Fd, Offset, State#mqistate{ fds = OpenFds#{ Segment => Fd }}}
    end.

locate(SeqId, SegmentEntryCount) ->
    Segment = SeqId div SegmentEntryCount,
    Offset = ?HEADER_SIZE + (SeqId rem SegmentEntryCount) * ?ENTRY_SIZE,
    {Segment, Offset}.

%% When recovering from a dirty shutdown, we may end up reading entries that
%% have already been acked. We do not add them to the Acc in that case, and
%% as a result we may end up returning less messages than initially expected.

parse_entries(<<>>, _, Acc) ->
    Acc;
parse_entries(<< Status:8,
                 IsDelivered:8,
                 _:7, IsPersistent:1,
                 _:8,
                 Id0:128,
                 Size:32/unsigned,
                 Expiry0:64/unsigned,
                 Rest/bits >>, SeqId, Acc) ->
    %% We skip entries that have already been acked. This may only
    %% happen when we recover from a dirty shutdown.
    case Status of
        1 ->
            %% We get the Id binary in two steps because we do not want
            %% to create a sub-binary and keep the larger binary around
            %% in memory.
            Id = <<Id0:128>>,
            Expiry = case Expiry0 of
                0 -> undefined;
                _ -> Expiry0
            end,
            Props = #message_properties{expiry = Expiry, size = Size},
            %% @todo We need to check if we have a 'deliver' in the write buffer as well.
            parse_entries(Rest, SeqId + 1, [{Id, SeqId, Props, IsPersistent =:= 1, IsDelivered =:= 1}|Acc]);
        2 ->
            %% @todo It would be good to keep track of how many "misses"
            %%       we have. We can use it to confirm the correct behavior
            %%       of the module in tests, as well as an occasionally
            %%       useful internal metric. Maybe use counters.
            parse_entries(Rest, SeqId + 1, Acc)
    end.

%% ----
%%
%% Defer to rabbit_queue_index for recovery for the time being.
%% @todo This is most certainly wrong.

start(VHost, DurableQueueNames) ->
    ?DEBUG("~0p ~0p", [VHost, DurableQueueNames]),
    rabbit_queue_index:start(VHost, DurableQueueNames).

stop(VHost) ->
    ?DEBUG("~0p", [VHost]),
    rabbit_queue_index:stop(VHost).

%% ----
%%
%% These functions either call the normal functions or are no-ops.
%% They relate to specific optimizations of rabbit_queue_index and
%% rabbit_variable_queue.

pre_publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State) ->
    ?DEBUG("~0p ~0p ~0p ~0p ~0p ~0p", [MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State]),
    publish(MsgOrId, SeqId, Props, IsPersistent, IsDelivered, TargetRamCount, State).
    %% @todo I don't know what this is but we only want to write if we have never
    %% written before? And we want to write in the right order.
    %% @todo Do something about IsDelivered too? I don't understand this function.
    %% I think it's to allow sending the message to the consumer before writing to the index.
    %% If it is delivered then we need to increase our read_marker. If that doesn't work
    %% then we can always keep a list of delivered messages in memory? -> probably necessary

%% @todo -spec flush_pre_publish_cache(???, State) -> State when State::mqistate().

flush_pre_publish_cache(TargetRamCount, State) ->
    ?DEBUG("~0p ~0p", [TargetRamCount, State]),
    State.

-spec sync(State) -> State when State::mqistate().

%% @todo Move this elsewhere.
sync(State0 = #mqistate{ confirms = Confirms,
                         fds = OpenFds,
                         on_sync = OnSyncFun }) ->
    ?DEBUG("~0p", [State0]),
    State = flush_buffer(State0, full),
    %% Call file:sync/1 on all open FDs. Some of them might not have
    %% writes but for the time being we don't discriminate.
    _ = maps:fold(fun(_, Fd, _) ->
        ok = file:sync(Fd)
    end, undefined, OpenFds),
    %% Notify syncs.
    Set = gb_sets:from_list(maps:values(Confirms)),
    OnSyncFun(Set),
    %% Reset confirms.
    State#mqistate{ confirms = #{} }.

-spec needs_sync(mqistate()) -> 'false'.

%% @todo Move this elsewhere.
needs_sync(State = #mqistate{ confirms = Confirms }) ->
    ?DEBUG("~0p", [State]),
    case Confirms =:= #{} of
        true -> false;
        false -> confirms
    end.

-spec flush(State) -> State when State::mqistate().

%% @todo We might need this as well? Lower memory usage before hibernate.
flush(State) ->
    ?DEBUG("~0p", [State]),
    State.

%% See comment in rabbit_queue_index:bounds/1. We do not need to be
%% accurate about these values because they are simply used as lowest
%% and highest possible bounds.

-spec bounds(State) ->
                       {non_neg_integer(), non_neg_integer(), State}
                       when State::mqistate().

bounds(State = #mqistate{ write_marker = Newest,
                          read_marker = Oldest }) ->
    ?DEBUG("~0p", [State]),
    {Oldest, Newest, State}.

%% The next_segment_boundary/1 function is used internally when
%% reading. It should not be called from rabbit_variable_queue.

-spec next_segment_boundary(SeqId) -> SeqId when SeqId::seq_id().

next_segment_boundary(SeqId) ->
    ?DEBUG("~0p", [SeqId]),
    SegmentEntryCount = segment_entry_count(),
    (1 + (SeqId div SegmentEntryCount)) * SegmentEntryCount.

%% ----
%%
%% Internal.

segment_entry_count() ->
    %% @todo Figure out what the best default would be.
    %%       A value lower than the max write_buffer size results in nothing needing
    %%       to be written to disk as long as the consumer consumes as fast as the
    %%       producer produces.
    SegmentEntryCount =
        application:get_env(rabbit, modern_queue_index_segment_entry_count, 65536),
    SegmentEntryCount.

erase_index_dir(Dir) ->
    case rabbit_file:is_dir(Dir) of
        true  -> rabbit_file:recursive_delete([Dir]);
        false -> ok
    end.

queue_dir(VHostDir, QueueName) ->
    %% Queue directory is
    %% {node_database_dir}/msg_stores/vhosts/{vhost}/queues/{queue}
    QueueDir = queue_name_to_dir_name(QueueName),
    filename:join([VHostDir, "queues", QueueDir]).

queue_name_to_dir_name(#resource { kind = queue,
                                   virtual_host = VHost,
                                   name = QName }) ->
    <<Num:128>> = erlang:md5(<<"queue", VHost/binary, QName/binary>>),
    rabbit_misc:format("~.36B", [Num]).

segment_file(Segment, #mqistate{ dir = Dir }) ->
    filename:join(Dir, integer_to_list(Segment) ++ ?SEGMENT_EXTENSION).

highest_continuous_seq_id([SeqId1, SeqId2|Tail])
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail]);
highest_continuous_seq_id([SeqId|_]) ->
    SeqId.

highest_continuous_seq_id([SeqId|Tail], EndSeqId)
        when (1 + SeqId) =:= EndSeqId ->
    {SeqId, Tail};
highest_continuous_seq_id([SeqId1, SeqId2|Tail], EndSeqId)
        when (1 + SeqId1) =:= SeqId2 ->
    highest_continuous_seq_id([SeqId2|Tail], EndSeqId);
highest_continuous_seq_id([SeqId|Tail], _) ->
    {SeqId, Tail}.
