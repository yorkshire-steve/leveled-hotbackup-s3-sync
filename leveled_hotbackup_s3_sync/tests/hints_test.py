import tempfile

import cdblib

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.hints import create_hints_file, get_sqn


def test_create_hints_file():
    journal_keys = [
        (
            101,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                erlang.OtpErlangBinary(b"testBucket"),
                erlang.OtpErlangBinary(b"testKey1"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
        (
            102,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                erlang.OtpErlangBinary(b"testBucket"),
                erlang.OtpErlangBinary(b"testKey2"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
        (
            103,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                erlang.OtpErlangBinary(b"testBucket"),
                erlang.OtpErlangBinary(b"testKey3"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
        (
            104,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey1"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
        (
            105,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey2"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
    ]
    with tempfile.NamedTemporaryFile() as file_handle:
        create_hints_file(file_handle.name, journal_keys)

        with cdblib.Reader.from_file_path(file_handle.name) as reader:
            assert reader.keys() == [
                b"testBucket\0testKey1",
                b"testBucket\0testKey2",
                b"testBucket\0testKey3",
                b"testType\0typedBucket\0typedKey1",
                b"testType\0typedBucket\0typedKey2",
            ]
            assert reader.getint(b"testBucket\0testKey1", 101)
            assert reader.getint(b"testBucket\0testKey2", 102)
            assert reader.getint(b"testBucket\0testKey3", 103)
            assert reader.getint(b"testType\0typedBucket\0typedKey1", 104)
            assert reader.getint(b"testType\0typedBucket\0typedKey2", 105)


def test_get_sqn():
    with tempfile.NamedTemporaryFile() as file_handle:
        with cdblib.Writer(file_handle) as writer:
            writer.putint(b"testBucket\0testKey1", 123)
            writer.putint(b"testBucket\0testKey1000", 1230)
            writer.putint(b"testType\0typedBucket\0typedKey933", 456)
        file_handle.flush()
        assert get_sqn(file_handle.name, b"testBucket", b"testKey1") == 123
        assert get_sqn(file_handle.name, b"testBucket", b"testKey1000") == 1230
        assert get_sqn(file_handle.name, b"typedBucket", b"typedKey933", b"testType") == 456
