# Python 2.5 features
from __future__ import with_statement

"""
Run Dispersy in standalone tracker mode.

Outputs statistics every 300 seconds:
- BANDWIDTH BYTES-UP BYTES-DOWN
- CANDIDATE COUNT(CANDIDATES)
- COMMUNITY COUNT(OVERLAYS) COUNT(KILLED-OVERLAYS)

Outputs active peers whenever encountered:
- REQ_IN2 HEX(COMMUNITY) hex(MEMBER) DISPERSY-VERSION OVERLAY-VERSION ADDRESS PORT
- RES_IN2 HEX(COMMUNITY) hex(MEMBER) DISPERSY-VERSION OVERLAY-VERSION ADDRESS PORT

Outputs destroyed communities whenever encountered:
- DESTROY_IN HEX(COMMUNITY) hex(MEMBER) DISPERSY-VERSION OVERLAY-VERSION ADDRESS PORT
- DESTROY_OUT HEX(COMMUNITY) hex(MEMBER) DISPERSY-VERSION OVERLAY-VERSION ADDRESS PORT

Note that there is no output for REQ_IN2 for destroyed overlays.  Instead a DESTROY_OUT is given
whenever a introduction request is received for a destroyed overlay.
"""

if __name__ == "__main__":
    # Concerning the relative imports, from PEP 328:
    # http://www.python.org/dev/peps/pep-0328/
    #
    #    Relative imports use a module's __name__ attribute to determine that module's position in
    #    the package hierarchy. If the module's name does not contain any package information
    #    (e.g. it is set to '__main__') then relative imports are resolved as if the module were a
    #    top level module, regardless of where the module is actually located on the file system.
    print "Usage: python -c \"from dispersy.tool.tracker import main; main()\" [--statedir DIR] [--ip ADDR] [--port PORT]"
    exit(1)

from time import time
import os
import errno
import optparse
import signal
import sys

from ..callback import Callback
from ..candidate import BootstrapCandidate, LoopbackCandidate
from ..community import Community, HardKilledCommunity
from ..conversion import BinaryConversion
from ..crypto import ec_generate_key, ec_to_public_bin, ec_to_private_bin
from ..dispersy import Dispersy
from ..dprint import dprint
from ..endpoint import StandaloneEndpoint
from ..member import DummyMember, Member
from ..message import Message, DropMessage

if sys.platform == 'win32':
    SOCKET_BLOCK_ERRORCODE = 10035    # WSAEWOULDBLOCK
else:
    SOCKET_BLOCK_ERRORCODE = errno.EWOULDBLOCK

class BinaryTrackerConversion(BinaryConversion):
    def decode_message(self, candidate, data, _=None):
        # disable verify
        return self._decode_message(candidate, data, False, False)

class TrackerHardKilledCommunity(HardKilledCommunity):
    def __init__(self, *args, **kargs):
        super(TrackerHardKilledCommunity, self).__init__(*args, **kargs)
        # communities are cleaned based on a 'strike' rule.  periodically, we will check is there
        # are active candidates, when there are 'strike' is set to zero, otherwise it is incremented
        # by one.  once 'strike' reaches a predefined value the community is cleaned
        self._strikes = 0

    def update_strikes(self, now):
        # does the community have any active candidates
        self._strikes += 1
        return self._strikes

    def dispersy_on_introduction_request(self, messages):
        hex_cid = messages[0].community.cid.encode("HEX")
        for message in messages:
            host, port = message.candidate.sock_addr
            print "DESTROY_OUT", hex_cid, message.authentication.member.mid.encode("HEX"), ord(message.conversion.dispersy_version), ord(message.conversion.community_version), host, port
        return super(TrackerHardKilledCommunity, self).dispersy_on_introduction_request(messages)

class TrackerCommunity(Community):
    """
    This community will only use dispersy-candidate-request and dispersy-candidate-response messages.
    """
    def __init__(self, *args, **kargs):
        super(TrackerCommunity, self).__init__(*args, **kargs)
        # communities are cleaned based on a 'strike' rule.  periodically, we will check is there
        # are active candidates, when there are 'strike' is set to zero, otherwise it is incremented
        # by one.  once 'strike' reaches a predefined value the community is cleaned
        self._strikes = 0

        self._walked_stumbled_candidates = self._iter_categories([u'walk', u'stumble'])

    def _initialize_meta_messages(self):
        super(TrackerCommunity, self)._initialize_meta_messages()

        # remove all messages that we should not be using
        meta_messages = self._meta_messages
        self._meta_messages = {}
        for name in [u"dispersy-introduction-request",
                     u"dispersy-introduction-response",
                     u"dispersy-puncture-request",
                     u"dispersy-puncture",
                     u"dispersy-identity",
                     u"dispersy-missing-identity",

                     u"dispersy-authorize",
                     u"dispersy-revoke",
                     u"dispersy-missing-proof",
                     u"dispersy-destroy-community"]:
            self._meta_messages[name] = meta_messages[name]

    @property
    def dispersy_auto_download_master_member(self):
        return False

    @property
    def dispersy_sync_bloom_filter_strategy(self):
        # disable sync bloom filter
        return lambda: None

    @property
    def dispersy_acceptable_global_time_range(self):
        # we will accept the full 64 bit global time range
        return 2**64 - self._global_time

    def update_strikes(self, now):
        # does the community have any active candidates
        for candidate in self._dispersy.candidates:
            if candidate.is_active(self, now):
                self._strikes = 0
                break
        else:
            self._strikes += 1
        return self._strikes

    def initiate_meta_messages(self):
        return []

    def initiate_conversions(self):
        return [BinaryTrackerConversion(self, "\x00")]

    def get_conversion(self, prefix=None):
        if not prefix in self._conversions:

            # the dispersy version MUST BE available.  Currently we
            # only support \x00: BinaryConversion
            if prefix[0] == "\x00":
                self._conversions[prefix] = BinaryTrackerConversion(self, prefix[1])

            else:
                raise KeyError("Unknown conversion")

            # use highest version as default
            if None in self._conversions:
                if self._conversions[None].version < self._conversions[prefix].version:
                    self._conversions[None] = self._conversions[prefix]
            else:
                self._conversions[None] = self._conversions[prefix]

        return self._conversions[prefix]

    def dispersy_cleanup_community(self, message):
        # since the trackers use in-memory databases, we need to store the destroy-community
        # message, and all associated proof, separately.
        host, port = message.candidate.sock_addr
        print "DESTROY_IN", self._cid.encode("HEX"), message.authentication.member.mid.encode("HEX"), ord(message.conversion.dispersy_version), ord(message.conversion.community_version), host, port

        write = open(self._dispersy.persistent_storage_filename, "a+").write
        write("# received dispersy-destroy-community from %s\n" % (str(message.candidate),))

        identity_id = self._meta_messages[u"dispersy-identity"].database_id
        execute = self._dispersy.database.execute
        messages = [message]
        stored = set()
        while messages:
            message = messages.pop()

            if not message.packet in stored:
                stored.add(message.packet)
                write(" ".join((message.name, message.packet.encode("HEX"), "\n")))

                if not message.authentication.member.public_key in stored:
                    try:
                        packet, = execute(u"SELECT packet FROM sync WHERE meta_message = ? AND member = ?", (identity_id, message.authentication.member.database_id)).next()
                    except StopIteration:
                        pass
                    else:
                        write(" ".join(("dispersy-identity", str(packet).encode("HEX"), "\n")))

                _, proofs = self._timeline.check(message)
                messages.extend(proofs)

        return TrackerHardKilledCommunity

    def dispersy_yield_introduce_candidates(self, candidate=None):
        """
        Yields unique active candidates that are part of COMMUNITY in Round Robin (Not random anymore).
        """
        assert all(not sock_address in self._candidates for sock_address in self._dispersy._bootstrap_candidates.iterkeys()), "none of the bootstrap candidates may be in self._candidates"
        prev_result = None
        while True:
            result = self._walked_stumbled_candidates.next()
            if prev_result == result:
                if __debug__: dprint("yielding random ", None)
                yield None

            else:
                prev_result = result
                if result == candidate:
                    continue
                if __debug__: dprint("yielding random ", result)
                yield result

class TrackerDispersy(Dispersy):
    @classmethod
    def get_instance(cls, *args, **kargs):
        kargs["singleton_placeholder"] = Dispersy
        return super(TrackerDispersy, cls).get_instance(*args, **kargs)

    def __init__(self, callback, working_directory, silent = False):
        super(TrackerDispersy, self).__init__(callback, working_directory, u":memory:")

        # non-autoload nodes
        self._non_autoload = set()
        self._non_autoload.update(host for host, _ in self._bootstrap_candidates.iterkeys())
        # leaseweb machines, some are running boosters, they never unload a community
        self._non_autoload.update(["95.211.105.65", "95.211.105.67", "95.211.105.69", "95.211.105.71", "95.211.105.73", "95.211.105.75", "95.211.105.77", "95.211.105.79", "95.211.105.81", "85.17.81.36"])

        # generate a new my-member
        ec = ec_generate_key(u"very-low")
        self._my_member = Member(ec_to_public_bin(ec), ec_to_private_bin(ec))

        # location of persistent storage
        self._persistent_storage_filename = os.path.join(working_directory, "persistent-storage.data")
        self._silent = silent

        callback.register(self._load_persistent_storage)
        callback.register(self._unload_communities)

        if not self._silent:
            callback.register(self._report_statistics)

    @property
    def persistent_storage_filename(self):
        return self._persistent_storage_filename

    def get_community(self, cid, load=False, auto_load=True):
        try:
            return super(TrackerDispersy, self).get_community(cid, True, True)
        except KeyError:
            self._communities[cid] = TrackerCommunity.join_community(DummyMember(cid), self._my_member)
            return self._communities[cid]

    def _load_persistent_storage(self):
        # load all destroyed communities
        try:
            packets = [packet.decode("HEX") for _, packet in (line.split() for line in open(self._persistent_storage_filename, "r") if not line.startswith("#"))]
        except IOError:
            pass
        else:
            candidate = LoopbackCandidate()
            for packet in reversed(packets):
                try:
                    self.on_incoming_packets([(candidate, packet)], cache=False, timestamp=time())
                except:
                    dprint("Error while loading from persistent-destroy-community.data", exception=True, level="error")

    def _convert_packets_into_batch(self, packets):
        """
        Ensure that communities are loaded when the packet is received from a non-bootstrap node,
        otherwise, load and auto-load are disabled.
        """
        def filter_non_bootstrap_nodes():
            for candidate, packet in packets:
                cid = packet[2:22]

                if not cid in self._communities and False:#candidate.sock_addr[0] in self._non_autoload:
                    if __debug__:
                        dprint("drop a ", len(packet), " byte packet (received from non-autoload node) from ", candidate, level="warning", force=1)
                        self._statistics.dict_inc(self._statistics.drop, "_convert_packets_into_batch:from bootstrap node for unloaded community")
                    continue

                yield candidate, packet

        packets = list(filter_non_bootstrap_nodes())
        if packets:
            return super(TrackerDispersy, self)._convert_packets_into_batch(packets)

        else:
            return []

    def _unload_communities(self):
        def is_active(community, now):
            # check 1: does the community have any active candidates
            if community.update_strikes(now) < 3:
                return True

            # check 2: does the community have any cached messages waiting to be processed
            for meta in self._batch_cache.iterkeys():
                if meta.community == community:
                    return True

            # the community is inactive
            return False

        while True:
            yield 180.0
            now = time()
            inactive = [community for community in self._communities.itervalues() if not is_active(community, now)]
            if __debug__: dprint("cleaning ", len(inactive), "/", len(self._communities), " communities")
            for community in inactive:
                community.unload_community()

    def _report_statistics(self):
        while True:
            yield 300.0
            mapping = {TrackerCommunity:0, TrackerHardKilledCommunity:0}
            for community in self._communities.itervalues():
                mapping[type(community)] += 1

            print "BANDWIDTH", self._endpoint.total_up, self._endpoint.total_down
            print "COMMUNITY", mapping[TrackerCommunity], mapping[TrackerHardKilledCommunity]
            print "CANDIDATE", len(self._candidates)

            if self._statistics.outgoing:
                for key, value in self._statistics.outgoing.iteritems():
                    print "OUTGOING", key, value

    def create_introduction_request(self, community, destination, allow_sync, forward=True):
        # prevent steps towards other trackers
        if not isinstance(destination, BootstrapCandidate):
            return super(TrackerDispersy, self).create_introduction_request(community, destination, allow_sync, forward)

    def check_introduction_request(self, messages):
        for message in super(TrackerDispersy, self).check_introduction_request(messages):
            if isinstance(message, Message.Implementation) and isinstance(message.candidate, BootstrapCandidate):
                yield DropMessage(message, "drop dispersy-introduction-request from bootstrap peer")
                continue

            yield message

    def on_introduction_request(self, messages):
        if not self._silent:
            hex_cid = messages[0].community.cid.encode("HEX")
            for message in messages:
                host, port = message.candidate.sock_addr
                print "REQ_IN2", hex_cid, message.authentication.member.mid.encode("HEX"), ord(message.conversion.dispersy_version), ord(message.conversion.community_version), host, port
        return super(TrackerDispersy, self).on_introduction_request(messages)

    def on_introduction_response(self, messages):
        if not self._silent:
            hex_cid = messages[0].community.cid.encode("HEX")
            for message in messages:
                host, port = message.candidate.sock_addr
                print "RES_IN2", hex_cid, message.authentication.member.mid.encode("HEX"), ord(message.conversion.dispersy_version), ord(message.conversion.community_version), host, port
        return super(TrackerDispersy, self).on_introduction_response(messages)

def main():
    command_line_parser = optparse.OptionParser()
    command_line_parser.add_option("--profiler", action="store_true", help="use cProfile on the Dispersy thread", default=False)
    command_line_parser.add_option("--memory-dump", action="store_true", help="use meliae to dump the memory periodically", default=False)
    command_line_parser.add_option("--statedir", action="store", type="string", help="Use an alternate statedir", default=".")
    command_line_parser.add_option("--ip", action="store", type="string", default="0.0.0.0", help="Dispersy uses this ip")
    command_line_parser.add_option("--port", action="store", type="int", help="Dispersy uses this UDL port", default=6421)
    command_line_parser.add_option("--silent", action="store_true", help="Prevent tracker printing to console", default=False)

    # parse command-line arguments
    opt, _ = command_line_parser.parse_args()

    # start Dispersy
    dispersy = TrackerDispersy.get_instance(Callback(), unicode(opt.statedir), bool(opt.silent))
    dispersy.endpoint = StandaloneEndpoint(dispersy, opt.port, opt.ip)
    dispersy.endpoint.start()
    dispersy.define_auto_load(TrackerCommunity)
    dispersy.define_auto_load(TrackerHardKilledCommunity)

    def signal_handler(sig, frame):
        print "Received signal '", sig, "' in", frame, "(shutting down)"
        dispersy.callback.stop(wait=False)
    signal.signal(signal.SIGINT, signal_handler)

    # wait forever
    dispersy.callback.loop()
    dispersy.endpoint.stop()
