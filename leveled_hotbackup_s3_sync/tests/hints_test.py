import tempfile

import cdblib

from leveled_hotbackup_s3_sync.erlang import OtpErlangBinary, term_to_binary
from leveled_hotbackup_s3_sync.hints import create_hints_file, get_sqn
from leveled_hotbackup_s3_sync.journal import list_keys


def test_create_hints_file():
    journal_keys = list_keys(
        "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb"
    )
    with tempfile.NamedTemporaryFile() as file_handle:
        create_hints_file(file_handle.name, journal_keys)

        with cdblib.Reader.from_file_path(file_handle.name) as reader:
            assert len(reader.keys()) == 485
            assert (
                reader.getint(
                    term_to_binary(
                        (
                            (OtpErlangBinary(b"testType"), OtpErlangBinary(b"typedBucket")),
                            OtpErlangBinary(b"typedKey576"),
                        )
                    )
                )
                == 1440
            )
            assert (
                reader.getint(term_to_binary((OtpErlangBinary(b"testBucket"), OtpErlangBinary(b"testKey9185")))) == 1385
            )
            assert reader.getint(term_to_binary((OtpErlangBinary(b"testBucket"), OtpErlangBinary(b"testKey17")))) == 973


def test_get_sqn():
    with tempfile.NamedTemporaryFile() as file_handle:
        with cdblib.Writer(file_handle) as writer:
            writer.putint(term_to_binary((OtpErlangBinary(b"testBucket"), OtpErlangBinary(b"testKey1"))), 123)
            writer.putint(term_to_binary((OtpErlangBinary(b"testBucket"), OtpErlangBinary(b"testKey1000"))), 1230)
            writer.putint(
                term_to_binary(
                    ((OtpErlangBinary(b"testType"), OtpErlangBinary(b"typedBucket")), OtpErlangBinary(b"typedKey933"))
                ),
                456,
            )
        file_handle.flush()
        with cdblib.Reader.from_file_path(file_handle.name) as reader:
            assert get_sqn(reader, b"testBucket", b"testKey1") == 123
            assert get_sqn(reader, b"testBucket", b"testKey1000") == 1230
            assert get_sqn(reader, b"typedBucket", b"typedKey933", b"testType") == 456


def test_e2e_hints_files():
    journal_keys = list_keys(
        "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb"
    )
    with tempfile.NamedTemporaryFile() as file_handle:
        create_hints_file(file_handle.name, journal_keys)
        with cdblib.Reader.from_file_path(file_handle.name) as reader:
            assert get_sqn(reader, b"testBucket", b"testKey75") == 976
            assert get_sqn(reader, b"testBucket", b"testKey9120") == 1382
            assert get_sqn(reader, b"typedBucket", b"typedKey52", b"testType") == 1419
