"""
The Distribution policy that is assigned to a Meta Message determines *how* the message is
disseminate (if at all) between peers.  The following Distribution policies are currently defined:

- FullSyncDistribution: gossiped to every other peer in the overlay.

- LastSyncDistribution: the last N messages created by a each member are gossiped to every peer in
  the overlay.

- DirectDistribution: messages are not gossiped.

- RelayDistribution: not implemented.
"""

from abc import ABCMeta, abstractmethod
from .meta import MetaObject


class Pruning(MetaObject):

    class Implementation(MetaObject.Implementation):

        __metaclass__ = ABCMeta

        def __init__(self, meta, distribution):
            assert isinstance(distribution, SyncDistribution.Implementation), type(distribution)
            super(Pruning.Implementation, self).__init__(meta)
            self._distribution = distribution

        def get_state(self):
            if self.is_active():
                return "active"
            if self.is_inactive():
                return "inactive"
            if self.is_pruned():
                return "pruned"
            raise RuntimeError("Unable to obtain pruning state")

        @abstractmethod
        def is_active(self):
            pass

        @abstractmethod
        def is_inactive(self):
            pass

        @abstractmethod
        def is_pruned(self):
            pass


class NoPruning(Pruning):

    class Implementation(Pruning.Implementation):

        def is_active(self):
            return True

        def is_inactive(self):
            return False

        def is_pruned(self):
            return False


class GlobalTimePruning(Pruning):

    class Implementation(Pruning.Implementation):

        @property
        def inactive_threshold(self):
            return self._meta.inactive_threshold

        @property
        def prune_threshold(self):
            return self._meta.prune_threshold

        def is_active(self):
            return self._distribution.community.global_time - self._distribution.global_time < self._meta.inactive_threshold

        def is_inactive(self):
            return self._meta.inactive_threshold <= self._distribution.community.global_time - self._distribution.global_time < self._meta.prune_threshold

        def is_pruned(self):
            return self._meta.prune_threshold <= self._distribution.community.global_time - self._distribution.global_time

    def __init__(self, inactive, pruned):
        """
        Construct a new GlobalTimePruning object.

        INACTIVE is the number at which the message goes from state active to inactive.
        PRUNED is the number at which the message goes from state inactive to pruned.

        A message has the following states:
        - active:   current_global_time - message_global_time < INACTIVE
        - inactive: INACTIVE <= current_global_time - message_global_time < PRUNED
        - pruned:  PRUNED <= current_global_time - message_global_time
        """
        assert isinstance(inactive, int), type(inactive)
        assert isinstance(pruned, int), type(pruned)
        assert 0 < inactive < pruned, [inactive, pruned]
        super(GlobalTimePruning, self).__init__()
        self._inactive_threshold = inactive
        self._prune_threshold = pruned

    @property
    def inactive_threshold(self):
        return self._inactive_threshold

    @property
    def prune_threshold(self):
        return self._prune_threshold


class Distribution(MetaObject):

    class Implementation(MetaObject.Implementation):

        def __init__(self, meta, global_time):
            assert isinstance(meta, Distribution)
            assert isinstance(global_time, (int, long))
            assert global_time > 0
            super(Distribution.Implementation, self).__init__(meta)
            # the last known global time + 1 (from the user who signed the
            # message)
            self._global_time = global_time

        @property
        def global_time(self):
            return self._global_time

    def setup(self, message):
        """
        Setup is called after the meta message is initially created.
        """
        if __debug__:
            from .message import Message
        assert isinstance(message, Message)


class SyncDistribution(Distribution):

    """
    Allows gossiping and synchronization of messages thoughout the community.

    The PRIORITY value ranges [0:255] where the 0 is the lowest priority and 255 the highest.  Any
    messages that have a priority below 32 will not be synced.  These messages require a mechanism
    to request missing messages whenever they are needed.

    The PRIORITY was introduced when we found that the dispersy-identity messages are the majority
    of gossiped messages while very few are actually required.  The dispersy-missing-identity
    message is used to retrieve an identity whenever it is needed.

    The MEMBERS was introduced to allow a community to limit data dissemination.  When MEMBERS is an
    empty set all messages are synced, however, when MEMBERS contains one or more Member instances,
    only messages created by those members will be synced.  For example: adding only your own Member
    instance will disseminate only 0-hop messages.
    """

    class Implementation(Distribution.Implementation):

        def __init__(self, meta, global_time):
            super(SyncDistribution.Implementation, self).__init__(meta, global_time)
            self._pruning = meta.pruning.Implementation(meta.pruning, self)

        @property
        def community(self):
            return self._meta._community

        @property
        def synchronization_direction(self):
            return self._meta._synchronization_direction

        @property
        def synchronization_direction_id(self):
            return self._meta._synchronization_direction_id

        @property
        def priority(self):
            return self._meta._priority

        @property
        def database_id(self):
            return self._meta._database_id

        @property
        def pruning(self):
            return self._pruning

        @property
        def members(self):
            return self._meta.members

    def __init__(self, synchronization_direction, priority, pruning=NoPruning(), members_func=None):
        # note: messages with a high priority value are synced before those with a low priority
        # value.
        # note: the priority has precedence over the global_time based ordering.
        # note: the default priority should be 127, use higher or lowe values when needed.
        assert isinstance(synchronization_direction, unicode), type(synchronization_direction)
        assert synchronization_direction in (u"ASC", u"DESC", u"RANDOM"), synchronization_direction
        assert isinstance(priority, int), type(priority)
        assert 0 <= priority <= 255, priority
        assert isinstance(pruning, Pruning), type(pruning)
        assert members_func is None or callable(members_func), members_func
        self._synchronization_direction = synchronization_direction
        self._priority = priority
        self._current_sequence_number = 0
        self._pruning = pruning
        # self._members = set()
        self._members_func = members_func

    @property
    def community(self):
        return self._community

    @property
    def synchronization_direction(self):
        return self._synchronization_direction

    @property
    def synchronization_direction_value(self):
        return {u"ASC":1, u"DESC":-1, u"RANDOM":0}[self._synchronization_direction]

    @property
    def priority(self):
        return self._priority

    @property
    def pruning(self):
        return self._pruning

    # @property
    # def members(self):
    #     if __debug__:
    #         from .member import Member
    #         assert all(isinstance(member, Member) for member in self._members), [type(member) for member in self._members]
    #     return self._members

    def get_members(self, candidate=None):
        """
        Returns the result of MEMBERS_FUN which must be a tuple/list/set with Member instances for
        whom messages must be included in the sync.

        When CANDIDATE is None we should return the list of Members that we want to disseminate
        ourselves (this is called on an incoming introduction request, where we must select messages
        to disseminate).

        When CANDIDATE is given, we should return the list of Members that CANDIDATE wants to
        disseminate (this is called when we create an introduction request, and we want to fill our
        bloom filter with messages that we know CANDIDATE will want to send back).
        """
        return self._members_func(candidate) if self._members_func else []

    def set_zero_hop_distribution(self):
        """
        Convenience function that sets a MEMBERS_FUNC which ensures zero hop dissemination.
        """
        self._members_func = self._zero_hop_distribution

    def _zero_hop_distribution(self, candidate):
        """
        Returns a tuple/list/set with Member instances for whom messages must be included in the
        sync.

        Returns [Community.my_member] when CANDIDATE is None, otherwise the Members that are
        associated with CANDIDATE.
        """
        if __debug__:
            from .candidate import WalkCandidate
            assert candidate is None or isinstance(candidate, WalkCandidate), type(candidate)
        if candidate:
            # return the Member(s) associated with CANDIDATE
            # TODO when we haven't met the CANDIDATE before, we won't know the associated Member
            return candidate.get_members()

        else:
            return [self._community.my_member]

    def setup(self, message):
        """
        Setup is called after the meta message is initially created.

        It is used to determine the current sequence number, based on
        which messages are already in the database.
        """
        if __debug__:
            from .message import Message
        assert isinstance(message, Message)

        # pruning requires information from the community
        self._community = message.community

        # use cache to avoid database queries
        assert message.name in message.community.meta_message_cache
        cache = message.community.meta_message_cache[message.name]
        if not (cache["priority"] == self._priority and cache["direction"] == self.synchronization_direction_value):
            message.community.dispersy.database.execute(u"UPDATE meta_message SET priority = ?, direction = ? WHERE id = ?",
                                                        (self._priority, self.synchronization_direction_value, message.database_id))
            assert message.community.dispersy.database.changes == 1


class FullSyncDistribution(SyncDistribution):

    """
    Allows gossiping and synchronization of messages thoughout the community.

    Sequence numbers can be enabled or disabled per meta-message.  When disabled the sequence number
    is always zero.  When enabled the claim_sequence_number method can be called to obtain the next
    requence number in sequence.

    Currently there is one situation where disabling sequence numbers is required.  This is when the
    message will be signed by multiple members.  In this case the sequence number is claimed but may
    not be used (if the other members refuse to add their signature).  This causes a missing
    sequence message.  This in turn could be solved by creating a placeholder message, however, this
    is not currently, and my never be, implemented.
    """
    class Implementation(SyncDistribution.Implementation):

        def __init__(self, meta, global_time, sequence_number=0):
            assert isinstance(sequence_number, (int, long))
            assert (meta._enable_sequence_number and sequence_number > 0) or (not meta._enable_sequence_number and sequence_number == 0), (meta._enable_sequence_number, sequence_number)
            super(FullSyncDistribution.Implementation, self).__init__(meta, global_time)
            self._sequence_number = sequence_number

        @property
        def enable_sequence_number(self):
            return self._meta._enable_sequence_number

        @property
        def sequence_number(self):
            return self._sequence_number

    def __init__(self, synchronization_direction, priority, enable_sequence_number, pruning=NoPruning(), members_func=None):
        assert isinstance(enable_sequence_number, bool)
        super(FullSyncDistribution, self).__init__(synchronization_direction, priority, pruning, members_func)
        self._enable_sequence_number = enable_sequence_number

    @property
    def enable_sequence_number(self):
        return self._enable_sequence_number

    def setup(self, message):
        super(FullSyncDistribution, self).setup(message)
        if self._enable_sequence_number:
            # obtain the most recent sequence number that we have used
            self._current_sequence_number, = message.community.dispersy.database.execute(u"SELECT COUNT(1) FROM sync WHERE member = ? AND meta_message = ?",
                                                                                         (message.community.my_member.database_id, message.database_id)).next()
    def claim_sequence_number(self):
        assert self._enable_sequence_number
        self._current_sequence_number += 1
        return self._current_sequence_number


class LastSyncDistribution(SyncDistribution):

    class Implementation(SyncDistribution.Implementation):

        @property
        def history_size(self):
            return self._meta._history_size

    def __init__(self, synchronization_direction, priority, history_size, pruning=NoPruning(), members_func=None):
        assert isinstance(history_size, int), type(history_size)
        assert history_size > 0, history_size
        super(LastSyncDistribution, self).__init__(synchronization_direction, priority, pruning, members_func)
        self._history_size = history_size

    @property
    def history_size(self):
        return self._history_size


class DirectDistribution(Distribution):

    class Implementation(Distribution.Implementation):
        pass


class RelayDistribution(Distribution):

    class Implementation(Distribution.Implementation):
        pass
