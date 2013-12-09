import argparse
import struct

from dispersy.authentication import MemberAuthentication
from dispersy.callback import Callback
from dispersy.community import Community
from dispersy.conversion import DefaultConversion, BinaryConversion
from dispersy.destination import CommunityDestination
from dispersy.dispersy import Dispersy
from dispersy.distribution import FullSyncDistribution, DirectDistribution
from dispersy.endpoint import StandaloneEndpoint
from dispersy.message import Message, DelayMessageByProof
from dispersy.payload import Payload
from dispersy.resolution import LinearResolution, PublicResolution

class ChatCommunity(Community):
    def __init__(self, dispersy, master_member, nickname):
        super(ChatCommunity, self).__init__(dispersy, master_member)
        self._nickname = nickname

    def initiate_conversions(self):
        return [DefaultConversion(self), ChatConversion(self)]

    def initiate_meta_messages(self):
        return [Message(self,
                        u"text",
                        MemberAuthentication(encoding="bin"),
                        LinearResolution(),
                        FullSyncDistribution(enable_sequence_number=False, synchronization_direction=u"ASC", priority=128),
                        CommunityDestination(node_count=10),
                        TextPayload(),
                        self.check_text,
                        self.on_text),
                Message(self,
                        u"beg-permission",
                        MemberAuthentication(encoding="bin"),
                        PublicResolution(),
                        DirectDistribution(),
                        CommunityDestination(node_count=20),
                        BegPermissionPayload(),
                        self.check_beg_permission,
                        self.on_beg_permission)]

    def create_text(self, text):
        meta = self.get_meta_message(u"text")
        allowed, proof = self.timeline.allowed(meta)
        if allowed:
            message = meta.impl(authentication=(self.my_member,),
                                distribution=(self.claim_global_time(),),
                                payload=(self._nickname, text))
            self.dispersy.store_update_forward([message], True, True, True)
        else:
            print "Cannot send to overlay (permission denied)"

    def check_text(self, messages):
        for message in messages:
            allowed, proofs = self.timeline.check(message)
            if allowed:
                yield message
            else:
                yield DelayMessageByProof(message)

    def on_text(self, messages):
        for message in messages:
            print "@%-3d uid%-3d %10s says: %s" %\
                (message.distribution.global_time,
                 message.authentication.member.database_id,
                 message.payload.nickname,
                 message.payload.text)

    def create_beg_permission(self, voice, operator):
        meta = self.get_meta_message(u"beg-permission")
        message = meta.impl(authentication=(self.my_member,),
                            distribution=(self.claim_global_time(),),
                            payload=(voice, operator))
        self.dispersy.store_update_forward([message], False, False, True)

    def check_beg_permission(self, messages):
        meta = self.get_meta_message(u"text")
        allowed = (self.timeline.allowed(meta, permission=u"authorize") and
                   self.timeline.allowed(meta, permission=u"revoke"))

        for message in messages:
            if allowed:
                yield message
            else:
                yield DropMessage("Unable to grant or revoke permissions")

    def on_beg_permission(self, messages):
        meta = self.get_meta_message(u"text")
        for message in messages:
            print "on_beg_permission", message.payload.voice, message.payload.operator
            if message.payload.voice > 0:
                self.create_dispersy_authorize([(message.authentication.member, meta, u"permit")])

            elif message.payload.voice < 0:
                self.create_dispersy_revoke([(message.authentication.member, meta, u"permit")])

            if message.payload.operator > 0:
                self.create_dispersy_authorize([(message.authentication.member, meta, u"authorize"),
                                                (message.authentication.member, meta, u"revoke")])
            elif message.payload.operator < 0:
                self.create_dispersy_revoke([(message.authentication.member, meta, u"authorize"),
                                             (message.authentication.member, meta, u"revoke")])

    def dispersy_store(self, messages):
        descriptions = dict()
        descriptions[(u"dispersy-authorize", u"text", u"permit")] = u"#@%-3d uid%-3d granted voice right to uid%-3d (can now create text messages)"
        descriptions[(u"dispersy-authorize", u"text", u"authorize")] = u"#@%-3d uid%-3d granted operator rights to uid%-3d (can now grant and revoke voice and operator rights)"
        descriptions[(u"dispersy-revoke", u"text", u"permit")] = u"#@%-3d uid%-3d revoked voice right from uid%-3d (can no longer create text messages)"
        descriptions[(u"dispersy-revoke", u"text", u"authorize")] = u"#@%-3d uid%-3d revoked operator rights from uid%-3d (can no longer grant and revoke voice and operator rights)"

        for message in messages:
            if message.name in (u"dispersy-authorize", u"dispersy-revoke"):
                for member, meta, permission in message.payload.permission_triplets:
                    description = descriptions.get((message.name, meta.name, permission))
                    if description:
                        print description %\
                            (message.distribution.global_time,
                             message.authentication.member.database_id,
                             member.database_id)

class TextPayload(Payload):
    class Implementation(Payload.Implementation):
        def __init__(self, meta, nickname, text):
            super(TextPayload.Implementation, self).__init__(meta)
            self.nickname = nickname
            self.text = text

class BegPermissionPayload(Payload):
    class Implementation(Payload.Implementation):
        def __init__(self, meta, voice, operator):
            super(BegPermissionPayload.Implementation, self).__init__(meta)
            self.voice = voice
            self.operator = operator

class ChatConversion(BinaryConversion):
    def __init__(self, community):
        super(ChatConversion, self).__init__(community, "\x01")
        self.define_meta_message(chr(1), community.get_meta_message(u"text"), self._encode_text, self._decode_text)
        self.define_meta_message(chr(2), community.get_meta_message(u"beg-permission"), self._encode_beg_permission, self._decode_beg_permission)

    def _encode_text(self, message):
        nickname = message.payload.nickname.encode("UTF-8")
        text = message.payload.text.encode("UTF-8")
        return struct.pack("!LL", len(nickname), len(text)), nickname, text

    def _decode_text(self, placeholder, offset, data):
        if len(data) < offset + 8:
            raise DropPacket("Insufficient packet size")

        nickname_length, text_length = struct.unpack_from("!LL", data, offset)
        offset += 8

        try:
            nickname = data[offset:offset+nickname_length].decode("UTF-8")
            offset += nickname_length

            text = data[offset:offset+text_length].decode("UTF-8")
            offset += text_length
        except UnicodeError:
            raise DropPacket("Unable to decode UTF-8")

        return offset, placeholder.meta.payload.implement(nickname, text)

    def _encode_beg_permission(self, message):
        return struct.pack("!bb", message.payload.voice, message.payload.operator),

    def _decode_beg_permission(self, placeholder, offset, data):
        if len(data) < offset + 2:
            raise DropPacket("Insufficient packet size")

        voice, operator = struct.unpack_from("!bb", data, offset)
        offset += 2

        return offset, placeholder.meta.payload.implement(voice, operator)

def create_chat_room(dispersy, nickname):
    my_member = dispersy.get_new_member()
    return ChatCommunity.create_community(dispersy, my_member, nickname)

def join_chat_room(dispersy, hash_, nickname):
    master = dispersy.get_temporary_member_from_id(hash_.decode("HEX"))
    my_member = dispersy.get_new_member()
    return ChatCommunity.join_community(dispersy, master, my_member, nickname)

def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-c", "--create", action="store_true", help="create a new chat room overlay")
    group.add_argument("-j", "--join", metavar="HASH", action="store", help="join an existing chat room by providing an overlay identifier")
    parser.add_argument("-p", "--port", action="store", type=int, default=3849, help="the UDP port that Dispersy should bind")
    parser.add_argument("-n", "--nickname", action="store", required=True, help="our nickname during chat")
    args = parser.parse_args()

    callback = Callback()
    endpoint = StandaloneEndpoint(args.port)
    dispersy = Dispersy(callback, endpoint, u".", u":memory:")
    dispersy.start()
    print "Dispersy is listening on port %d" % dispersy.lan_address[1]

    try:
        if args.create:
            community = callback.call(create_chat_room, (dispersy, unicode(args.nickname)))
            print "created chat room %s" % community.master_member.mid.encode("HEX")
        if args.join:
            community = callback.call(join_chat_room, (dispersy, args.join, unicode(args.nickname)))
            print "joined chat room %s" % community.master_member.mid.encode("HEX")

        while True:
            text = raw_input().strip()
            if text.startswith("/"):
                if text == "/+v":
                    callback.call(community.create_beg_permission, (1, 0))
                elif text == "/+o":
                    callback.call(community.create_beg_permission, (1, 1))
                elif text in ("/-v", "/-o"):
                    callback.call(community.create_beg_permission, (-1, -1))
            elif text:
                callback.call(community.create_text, (unicode(text),))

    except KeyboardInterrupt:
        print "shutdown"

    finally:
        dispersy.stop()

if __name__ == "__main__":
    main()

