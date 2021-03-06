from struct import pack, unpack_from

from .authentication import DoubleMemberAuthentication, MemberAuthentication
from .candidate import Candidate
from .community import Community, HardKilledCommunity
from .conversion import BinaryConversion, DefaultConversion
from .debug import Node
from .destination import MemberDestination, CommunityDestination
from .distribution import DirectDistribution, FullSyncDistribution, LastSyncDistribution
from .dprint import dprint
from .member import Member
from .message import Message, DropPacket, DelayMessageByProof
from .payload import Payload
from .resolution import PublicResolution, LinearResolution, DynamicResolution
from .revision import update_revision_information

# update version information directly from SVN
update_revision_information("$HeadURL$", "$Revision$")

#
# Node
#

class DebugNode(Node):
    def _create_text_message(self, message_name, text, global_time, resolution=(), destination=()):
        assert isinstance(message_name, unicode)
        assert isinstance(text, str)
        assert isinstance(global_time, (int, long))
        assert isinstance(resolution, tuple)
        assert isinstance(destination, tuple)
        meta = self._community.get_meta_message(message_name)
        return meta.impl(authentication=(self._my_member,),
                         resolution=resolution,
                         distribution=(global_time,),
                         destination=destination,
                         payload=(text,))

    def _create_sequence_text_message(self, message_name, text, global_time, sequence_number):
        assert isinstance(message_name, unicode)
        assert isinstance(text, str)
        assert isinstance(global_time, (int, long))
        assert isinstance(sequence_number, (int, long))
        meta = self._community.get_meta_message(message_name)
        return meta.impl(authentication=(self._my_member,),
                         distribution=(global_time, sequence_number),
                         payload=(text,))

    def _create_doublemember_text_message(self, message_name, other, text, global_time):
        assert isinstance(message_name, unicode)
        assert isinstance(other, Member)
        assert not self._my_member == other
        assert isinstance(text, str)
        assert isinstance(global_time, (int, long))
        meta = self._community.get_meta_message(message_name)
        return meta.impl(authentication=([self._my_member, other],),
                         distribution=(global_time,),
                         payload=(text,))

    def create_last_1_test_message(self, text, global_time):
        return self._create_text_message(u"last-1-test", text, global_time)

    def create_last_9_test_message(self, text, global_time):
        return self._create_text_message(u"last-9-test", text, global_time)

    def create_last_1_doublemember_text_message(self, other, text, global_time):
        return self._create_doublemember_text_message(u"last-1-doublemember-text", other, text, global_time)

    def create_full_sync_text_message(self, text, global_time):
        return self._create_text_message(u"full-sync-text", text, global_time)

    def create_in_order_text_message(self, text, global_time):
        return self._create_text_message(u"ASC-text", text, global_time)

    def create_out_order_text_message(self, text, global_time):
        return self._create_text_message(u"DESC-text", text, global_time)

    def create_protected_full_sync_text_message(self, text, global_time):
        return self._create_text_message(u"protected-full-sync-text", text, global_time)

    def create_dynamic_resolution_text_message(self, text, global_time, policy):
        assert isinstance(policy, (PublicResolution.Implementation, LinearResolution.Implementation))
        return self._create_text_message(u"dynamic-resolution-text", text, global_time, resolution=(policy,))

    def create_sequence_test_message(self, text, global_time, sequence_number):
        return self._create_sequence_text_message(u"sequence-text", text, global_time, sequence_number)
#
# Conversion
#

class DebugCommunityConversion(BinaryConversion):
    def __init__(self, community):
        super(DebugCommunityConversion, self).__init__(community, "\x02")
        self.define_meta_message(chr(1), community.get_meta_message(u"last-1-test"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(2), community.get_meta_message(u"last-9-test"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(4), community.get_meta_message(u"double-signed-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(8), community.get_meta_message(u"full-sync-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(9), community.get_meta_message(u"ASC-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(10), community.get_meta_message(u"DESC-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(11), community.get_meta_message(u"last-1-doublemember-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(12), community.get_meta_message(u"protected-full-sync-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(13), community.get_meta_message(u"dynamic-resolution-text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(14), community.get_meta_message(u"sequence-text"), self._encode_text, self._decode_text)

    def _encode_text(self, message):
        return pack("!B", len(message.payload.text)), message.payload.text

    def _decode_text(self, placeholder, offset, data):
        if len(data) < offset + 1:
            raise DropPacket("Insufficient packet size")

        text_length, = unpack_from("!B", data, offset)
        offset += 1

        if len(data) < offset + text_length:
            raise DropPacket("Insufficient packet size")

        text = data[offset:offset+text_length]
        offset += text_length

        return offset, placeholder.meta.payload.implement(text)

#
# Payload
#

class TextPayload(Payload):
    class Implementation(Payload.Implementation):
        def __init__(self, meta, text):
            assert isinstance(text, str)
            super(TextPayload.Implementation, self).__init__(meta)
            self._text = text

        @property
        def text(self):
            return self._text

#
# Community
#

class DebugCommunity(Community):
    """
    Community to debug Dispersy related messages and policies.
    """
    @property
    def my_candidate(self):
        return Candidate(self._dispersy.lan_address, False)

    @property
    def dispersy_candidate_request_initial_delay(self):
        # disable candidate
        return 0.0

    @property
    def dispersy_sync_initial_delay(self):
        # disable sync
        return 0.0

    def initiate_conversions(self):
        return [DefaultConversion(self), DebugCommunityConversion(self)]

    #
    # helper methods to check database status
    #

    def fetch_packets(self, *message_names):
        return [str(packet) for packet, in list(self._dispersy.database.execute(u"SELECT packet FROM sync WHERE meta_message IN (" + ", ".join("?" * len(message_names)) + ") ORDER BY global_time, packet",
                                                                                [self.get_meta_message(name).database_id for name in message_names]))]

    def fetch_messages(self, *message_names):
        """
        Fetch all packets for MESSAGE_NAMES from the database and converts them into
        Message.Implementation instances.
        """
        return self._dispersy.convert_packets_to_messages(self.fetch_packets(*message_names), community=self, verify=False)

    def delete_messages(self, *message_names):
        """
        Deletes all packets for MESSAGE_NAMES from the database.  Returns the number of packets
        removed.
        """
        self._dispersy.database.execute(u"DELETE FROM sync WHERE meta_message IN (" + ", ".join("?" * len(message_names)) + ")",
                                        [self.get_meta_message(name).database_id for name in message_names])
        return self._dispersy.database.changes

    def initiate_meta_messages(self):
        return [Message(self, u"last-1-test", MemberAuthentication(), PublicResolution(), LastSyncDistribution(synchronization_direction=u"ASC", priority=128, history_size=1), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"last-9-test", MemberAuthentication(), PublicResolution(), LastSyncDistribution(synchronization_direction=u"ASC", priority=128, history_size=9), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"last-1-doublemember-text", DoubleMemberAuthentication(allow_signature_func=self.allow_signature_func), PublicResolution(), LastSyncDistribution(synchronization_direction=u"ASC", priority=128, history_size=1), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"double-signed-text", DoubleMemberAuthentication(allow_signature_func=self.allow_double_signed_text), PublicResolution(), DirectDistribution(), MemberDestination(), TextPayload(), self.check_text, self.on_text),
                Message(self, u"full-sync-text", MemberAuthentication(), PublicResolution(), FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text, self.undo_text),
                Message(self, u"ASC-text", MemberAuthentication(), PublicResolution(), FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"DESC-text", MemberAuthentication(), PublicResolution(), FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"DESC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"protected-full-sync-text", MemberAuthentication(), LinearResolution(), FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text),
                Message(self, u"dynamic-resolution-text", MemberAuthentication(), DynamicResolution(PublicResolution(), LinearResolution()), FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text, self.undo_text),
                Message(self, u"sequence-text", MemberAuthentication(), PublicResolution(), FullSyncDistribution(enable_sequence_number=True, synchronization_direction=u"ASC", priority=128), CommunityDestination(node_count=10), TextPayload(), self.check_text, self.on_text, self.undo_text),
                ]

    def create_full_sync_text(self, text, store=True, update=True, forward=True):
        meta = self.get_meta_message(u"full-sync-text")
        message = meta.impl(authentication=(self._my_member,),
                            distribution=(self.claim_global_time(),),
                            payload=(text,))
        self._dispersy.store_update_forward([message], store, update, forward)
        return message

    #
    # double-signed-text
    #

    def create_double_signed_text(self, text, member, response_func, response_args=(), timeout=10.0, forward=True):
        meta = self.get_meta_message(u"double-signed-text")
        message = meta.impl(authentication=([self._my_member, member],),
                            distribution=(self.global_time,),
                            destination=(member,),
                            payload=(text,))
        return self.create_dispersy_signature_request(message, response_func, response_args, timeout, forward)

    def allow_double_signed_text(self, message):
        """
        Received a request to sign MESSAGE.
        """
        dprint(message, " \"", message.payload.text, "\"")
        assert message.payload.text in ("Allow=True", "Allow=False")
        return message.payload.text == "Allow=True"

    #
    # last-1-doublemember-text
    #
    def allow_signature_func(self, message):
        return True

    #
    # protected-full-sync-text
    #
    def create_protected_full_sync_text(self, text, store=True, update=True, forward=True):
        meta = self.get_meta_message(u"protected-full-sync-text")
        message = meta.impl(authentication=(self._my_member,),
                            distribution=(self.claim_global_time(),),
                            payload=(text,))
        self._dispersy.store_update_forward([message], store, update, forward)
        return message

    #
    # dynamic-resolution-text
    #
    def create_dynamic_resolution_text(self, text, store=True, update=True, forward=True):
        meta = self.get_meta_message(u"dynamic-resolution-text")
        message = meta.impl(authentication=(self._my_member,),
                            distribution=(self.claim_global_time(),),
                            payload=(text,))
        self._dispersy.store_update_forward([message], store, update, forward)
        return message

    #
    # sequence-text
    #
    def create_sequence_text(self, text, store=True, update=True, forward=True):
        meta = self.get_meta_message(u"sequence-text")
        message = meta.impl(authentication=(self._my_member,),
                            distribution=(self.claim_global_time(), meta.distribution.claim_sequence_number()),
                            payload=(text,))
        self._dispersy.store_update_forward([message], store, update, forward)
        return message

    #
    # any text-payload
    #

    def check_text(self, messages):
        for message in messages:
            allowed, proof = self._timeline.check(message)
            if allowed:
                yield message
            else:
                yield DelayMessageByProof(message)

    def on_text(self, messages):
        """
        Received a text message.
        """
        for message in messages:
            if not "Dprint=False" in message.payload.text:
                dprint(message, " \"", message.payload.text, "\" @", message.distribution.global_time)

    def undo_text(self, descriptors):
        """
        Received an undo for a text message.
        """
        for member, global_time, packet in descriptors:
            message = packet.load_message()
            dprint("undo \"", message.payload.text, "\" @", global_time)

    def dispersy_cleanup_community(self, message):
        if message.payload.is_soft_kill:
            raise NotImplementedError()

        elif message.payload.is_hard_kill:
            return HardKilledDebugCommunity

class HardKilledDebugCommunity(DebugCommunity, HardKilledCommunity):
    pass
