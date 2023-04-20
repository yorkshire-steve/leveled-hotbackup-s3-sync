#
# MIT License
#
# Copyright (c) 2023 Stephen Wood (@yorkshire-steve)
# Copyright (c) 2014-2022 Michael Truog <mjtruog at protonmail dot com>
# Copyright (c) 2009-2013 Dmitry Vasiliev <dima@hlabs.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
#
"""
Erlang External Term Format Encoding/Decoding Tests
"""

import pytest

from leveled_hotbackup_s3_sync import erlang


class TestAtom:
    def test_atom(self):
        atom1 = erlang.OtpErlangAtom("test")
        assert erlang.OtpErlangAtom == type(atom1)
        assert erlang.OtpErlangAtom("test") == atom1
        assert "OtpErlangAtom('test')" == repr(atom1)
        assert isinstance(atom1, erlang.OtpErlangAtom) is True
        atom2 = erlang.OtpErlangAtom("test2")
        atom1_new = erlang.OtpErlangAtom("test")
        assert (atom1 is atom2) is False
        assert hash(atom1) != hash(atom2)
        assert atom1 is not atom1_new
        assert hash(atom1) == hash(atom1_new)
        assert "X" * 255 == erlang.OtpErlangAtom("X" * 255).value
        assert "X" * 256 == erlang.OtpErlangAtom("X" * 256).value
        assert "\u0100" * 512 == erlang.OtpErlangAtom("\u0100" * 512).value
        with pytest.raises(erlang.OutputException):
            erlang.OtpErlangAtom("\u0100" * 32768).binary()
        with pytest.raises(erlang.OutputException):
            erlang.OtpErlangAtom(b"a" * 65536).binary()

    def test_invalid_atom(self):
        with pytest.raises(erlang.OutputException):
            erlang.OtpErlangAtom.binary(erlang.OtpErlangAtom([1, 2]))


class TestList:
    def test_list(self):
        lst = erlang.OtpErlangList([116, 101, 115, 116])
        assert isinstance(lst, erlang.OtpErlangList) is True
        assert erlang.OtpErlangList([116, 101, 115, 116]) == lst
        assert [116, 101, 115, 116] == lst.value
        assert "OtpErlangList([116, 101, 115, 116],improper=False)" == repr(lst)


class TestImproperList:
    def test_improper_list(self):
        lst = erlang.OtpErlangList([1, 2, 3, 4], improper=True)
        assert isinstance(lst, erlang.OtpErlangList) is True
        assert [1, 2, 3, 4] == lst.value
        assert 4 == lst.value[-1]
        assert "OtpErlangList([1, 2, 3, 4],improper=True)" == repr(lst)

    def test_comparison(self):
        lst = erlang.OtpErlangList([1, 2, 3, 4], improper=True)
        assert lst == lst
        assert lst == erlang.OtpErlangList([1, 2, 3, 4], improper=True)
        assert lst != erlang.OtpErlangList([1, 2, 3, 5], improper=True)
        assert lst != erlang.OtpErlangList([1, 2, 3], improper=True)

    def test_errors(self):
        with pytest.raises(erlang.OutputException):
            erlang.OtpErlangList.binary(erlang.OtpErlangList("invalid", improper=True))


class TestDecode:
    def test_binary_to_term(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83z")

    def test_binary_to_term_atom(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83d")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83d\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83d\0\1")
        assert erlang.OtpErlangAtom(b"") == erlang.binary_to_term(b"\x83d\0\0")
        assert erlang.OtpErlangAtom(b"") == erlang.binary_to_term(b"\x83s\0")
        assert erlang.OtpErlangAtom(b"test") == erlang.binary_to_term(b"\x83d\0\4test")
        assert erlang.OtpErlangAtom(b"test") == erlang.binary_to_term(b"\x83s\4test")
        assert erlang.OtpErlangAtom("name") == erlang.binary_to_term(b"\x83w\x04name")
        assert erlang.OtpErlangAtom("\u0100sdf") == erlang.binary_to_term(b"\x83w\x05\xc4\x80sdf")

    def test_binary_to_term_predefined_atoms(self):
        assert erlang.binary_to_term(b"\x83s\4true") is True
        assert erlang.binary_to_term(b"\x83s\5false") is False
        assert erlang.binary_to_term(b"\x83w\5false") is False
        assert erlang.binary_to_term(b"\x83s\11undefined") is None
        assert erlang.binary_to_term(b"\x83w\11undefined") is None
        assert erlang.binary_to_term(b"\x83d\0\11undefined") is None
        assert erlang.binary_to_term(b"\x83v\0\11undefined") is None

    def test_binary_to_term_empty_list(self):
        assert [] == erlang.binary_to_term(b"\x83j")

    def test_binary_to_term_string_list(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83k")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83k\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83k\0\1")
        assert b"" == erlang.binary_to_term(b"\x83k\0\0")
        assert b"test" == erlang.binary_to_term(b"\x83k\0\4test")

    def test_binary_to_term_list(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l\0\0\0\0")
        assert [] == erlang.binary_to_term(b"\x83l\0\0\0\0j")
        assert [[], []] == erlang.binary_to_term(b"\x83l\0\0\0\2jjj")

    def test_binary_to_term_improper_list(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83l\0\0\0\0k")
        lst = erlang.binary_to_term(b"\x83l\0\0\0\1jd\0\4tail")
        assert isinstance(lst, erlang.OtpErlangList) is True
        assert [[], erlang.OtpErlangAtom(b"tail")] == lst.value
        assert lst.improper is True

    def test_binary_to_term_map(self):
        # represents #{ {at,[hello]} => ok}
        binary = b"\x83t\x00\x00\x00\x01h\x02d\x00\x02atl\x00\x00\x00\x01d\x00\x05hellojd\x00\x02ok"
        map_with_list = erlang.binary_to_term(binary)
        assert isinstance(map_with_list, dict) is True

    def test_binary_to_term_small_tuple(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83h")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83h\1")
        assert () == erlang.binary_to_term(b"\x83h\0")
        assert ([], []) == erlang.binary_to_term(b"\x83h\2jj")

    def test_binary_to_term_large_tuple(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83i")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83i\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83i\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83i\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83i\0\0\0\1")
        assert () == erlang.binary_to_term(b"\x83i\0\0\0\0")
        assert ([], []) == erlang.binary_to_term(b"\x83i\0\0\0\2jj")

    def test_binary_to_term_small_integer(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83a")
        assert 0 == erlang.binary_to_term(b"\x83a\0")
        assert 255 == erlang.binary_to_term(b"\x83a\xff")

    def test_binary_to_term_integer(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83b")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83b\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83b\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83b\0\0\0")
        assert 0 == erlang.binary_to_term(b"\x83b\0\0\0\0")
        assert 2147483647 == erlang.binary_to_term(b"\x83b\x7f\xff\xff\xff")
        assert -2147483648 == erlang.binary_to_term(b"\x83b\x80\0\0\0")
        assert -1 == erlang.binary_to_term(b"\x83b\xff\xff\xff\xff")

    def test_binary_to_term_binary(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83m")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83m\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83m\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83m\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83m\0\0\0\1")
        assert erlang.OtpErlangBinary(b"") == erlang.binary_to_term(b"\x83m\0\0\0\0")
        assert erlang.OtpErlangBinary(b"data") == erlang.binary_to_term(b"\x83m\0\0\0\4data")

    def test_binary_to_term_float(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0\0\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83F\0\0\0\0\0\0\0")
        assert 0.0 == erlang.binary_to_term(b"\x83F\0\0\0\0\0\0\0\0")
        assert 1.5 == erlang.binary_to_term(b"\x83F?\xf8\0\0\0\0\0\0")

    def test_binary_to_term_small_big_integer(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83n")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83n\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83n\1\0")
        assert 0 == erlang.binary_to_term(b"\x83n\0\0")
        assert 6618611909121 == erlang.binary_to_term(b"\x83n\6\0\1\2\3\4\5\6")
        assert -6618611909121 == erlang.binary_to_term(b"\x83n\6\1\1\2\3\4\5\6")

    def test_binary_to_term_big_integer(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o\0\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83o\0\0\0\1\0")
        assert 0 == erlang.binary_to_term(b"\x83o\0\0\0\0\0")
        assert 6618611909121 == erlang.binary_to_term(b"\x83o\0\0\0\6\0\1\2\3\4\5\6")
        assert -6618611909121 == erlang.binary_to_term(b"\x83o\0\0\0\6\1\1\2\3\4\5\6")

    def test_binary_to_term_pid(self):
        pid_old_binary = (
            b"\x83\x67\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E\x6F"
            b"\x68\x6F\x73\x74\x00\x00\x00\x4E\x00\x00\x00\x00\x00"
        )
        pid_old = erlang.binary_to_term(pid_old_binary)
        assert isinstance(pid_old, erlang.OtpErlangPid) is True
        assert erlang.term_to_binary(pid_old) == b"\x83gd\0\rnonode@nohost\x00\x00\x00N" b"\x00\x00\x00\x00\x00"
        pid_new_binary = (
            b"\x83\x58\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E\x6F\x68"
            b"\x6F\x73\x74\x00\x00\x00\x4E\x00\x00\x00\x00\x00\x00\x00\x00"
        )
        pid_new = erlang.binary_to_term(pid_new_binary)
        assert isinstance(pid_new, erlang.OtpErlangPid) is True
        assert (
            erlang.term_to_binary(pid_new) == b"\x83Xd\0\rnonode@nohost\x00\x00\x00N"
            b"\x00\x00\x00\x00\x00\x00\x00\x00"
        )

    def test_binary_to_term_port(self):
        port_old_binary = (
            b"\x83\x66\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E\x6F\x68" b"\x6F\x73\x74\x00\x00\x00\x06\x00"
        )
        port_old = erlang.binary_to_term(port_old_binary)
        assert isinstance(port_old, erlang.OtpErlangPort) is True
        assert erlang.term_to_binary(port_old) == b"\x83fd\0\rnonode@nohost\x00\x00\x00\x06\x00"
        port_new_binary = (
            b"\x83\x59\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E\x6F\x68"
            b"\x6F\x73\x74\x00\x00\x00\x06\x00\x00\x00\x00"
        )
        port_new = erlang.binary_to_term(port_new_binary)
        assert isinstance(port_new, erlang.OtpErlangPort) is True
        assert erlang.term_to_binary(port_new) == b"\x83Yd\0\rnonode@nohost\x00\x00\x00\x06" b"\x00\x00\x00\x00"

    def test_binary_to_term_ref(self):
        ref_new_binary = (
            b"\x83\x72\x00\x03\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E"
            b"\x6F\x68\x6F\x73\x74\x00\x00\x03\xE8\x4E\xE7\x68\x00\x02\xA4"
            b"\xC8\x53\x40"
        )
        ref_new = erlang.binary_to_term(ref_new_binary)
        assert isinstance(ref_new, erlang.OtpErlangReference) is True
        assert (
            erlang.term_to_binary(ref_new) == b"\x83r\x00\x03d\0\rnonode@nohost\x00\x00\x03\xe8"
            b"N\xe7h\x00\x02\xa4\xc8S@"
        )
        ref_newer_binary = (
            b"\x83\x5A\x00\x03\x64\x00\x0D\x6E\x6F\x6E\x6F\x64\x65\x40\x6E"
            b"\x6F\x68\x6F\x73\x74\x00\x00\x00\x00\x00\x01\xAC\x03\xC7\x00"
            b"\x00\x04\xBB\xB2\xCA\xEE"
        )
        ref_newer = erlang.binary_to_term(ref_newer_binary)
        assert isinstance(ref_newer, erlang.OtpErlangReference) is True
        assert (
            erlang.term_to_binary(ref_newer) == b"\x83Z\x00\x03d\0\rnonode@nohost\x00\x00\x00\x00\x00"
            b"\x01\xac\x03\xc7\x00\x00\x04\xbb\xb2\xca\xee"
        )

    def test_binary_to_term_compressed_term(self):
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P\0\0\0\0")
        with pytest.raises(erlang.ParseException):
            erlang.binary_to_term(b"\x83P\0\0\0\x16\x78\xda\xcb\x66\x10\x49\xc1\2\0\x5d\x60\x08\x50")
        assert b"d" * 20 == erlang.binary_to_term(
            b"\x83P\0\0\0\x17\x78\xda\xcb\x66" b"\x10\x49\xc1\2\0\x5d\x60\x08\x50"
        )


class TestEncode:
    def test_term_to_binary_tuple(self):
        assert b"\x83h\0" == erlang.term_to_binary(())
        assert b"\x83h\2h\0h\0" == erlang.term_to_binary(((), ()))
        assert b"\x83h\xff" + b"h\0" * 255 == erlang.term_to_binary(tuple([()] * 255))
        assert b"\x83i\0\0\1\0" + b"h\0" * 256 == erlang.term_to_binary(tuple([()] * 256))

    def test_term_to_binary_binary(self):
        with pytest.raises(erlang.OutputException):
            erlang.term_to_binary(erlang.OtpErlangBinary(123))
        assert b"\x83m\x00\x00\x00\x04asdf" == erlang.term_to_binary(erlang.OtpErlangBinary(b"asdf"))
        assert b"\x83M\x00\x00\x00\x04\x05asdf" == erlang.term_to_binary(erlang.OtpErlangBinary(b"asdf", bits=5))
        assert "OtpErlangBinary(b'test',bits=8)" == repr(erlang.OtpErlangBinary(b"test", bits=8))

    def test_term_to_binary_empty_list(self):
        assert b"\x83j" == erlang.term_to_binary([])

    def test_term_to_binary_string_list(self):
        assert b"\x83j" == erlang.term_to_binary("")
        assert b"\x83k\0\1\0" == erlang.term_to_binary("\0")
        value = (
            b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r"
            b"\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a"
            b"\x1b\x1c\x1d\x1e\x1f !\"#$%&'()*+,-./0123456789:;<=>"
            b"?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopq"
            b"rstuvwxyz{|}~\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88"
            b"\x89\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95"
            b"\x96\x97\x98\x99\x9a\x9b\x9c\x9d\x9e\x9f\xa0\xa1\xa2"
            b"\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xab\xac\xad\xae\xaf"
            b"\xb0\xb1\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xbb\xbc"
            b"\xbd\xbe\xbf\xc0\xc1\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9"
            b"\xca\xcb\xcc\xcd\xce\xcf\xd0\xd1\xd2\xd3\xd4\xd5\xd6"
            b"\xd7\xd8\xd9\xda\xdb\xdc\xdd\xde\xdf\xe0\xe1\xe2\xe3"
            b"\xe4\xe5\xe6\xe7\xe8\xe9\xea\xeb\xec\xed\xee\xef\xf0"
            b"\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xfb\xfc\xfd\xfe\xff"
        )
        assert b"\x83k\1\0" + value == erlang.term_to_binary(value)

    def test_term_to_binary_list_basic(self):
        assert b"\x83\x6A" == erlang.term_to_binary([])
        assert b"\x83\x6C\x00\x00\x00\x01\x6A\x6A" == erlang.term_to_binary([""])
        assert b"\x83\x6C\x00\x00\x00\x01\x61\x01\x6A" == erlang.term_to_binary([1])
        assert b"\x83\x6C\x00\x00\x00\x01\x61\xFF\x6A" == erlang.term_to_binary([255])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\x00\x00\x01\x00\x6A" == erlang.term_to_binary([256])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\x7F\xFF\xFF\xFF\x6A" == erlang.term_to_binary([2147483647])
        assert b"\x83\x6C\x00\x00\x00\x01\x6E\x04\x00\x00\x00\x00\x80\x6A" == erlang.term_to_binary([2147483648])
        assert b"\x83\x6C\x00\x00\x00\x01\x61\x00\x6A" == erlang.term_to_binary([0])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\xFF\xFF\xFF\xFF\x6A" == erlang.term_to_binary([-1])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\xFF\xFF\xFF\x00\x6A" == erlang.term_to_binary([-256])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\xFF\xFF\xFE\xFF\x6A" == erlang.term_to_binary([-257])
        assert b"\x83\x6C\x00\x00\x00\x01\x62\x80\x00\x00\x00\x6A" == erlang.term_to_binary([-2147483648])
        assert b"\x83\x6C\x00\x00\x00\x01\x6E\x04\x01\x01\x00\x00\x80\x6A" == erlang.term_to_binary([-2147483649])
        assert b"\x83\x6C\x00\x00\x00\x01\x6B\x00\x04\x74\x65\x73\x74\x6A" == erlang.term_to_binary(["test"])
        assert b"\x83\x6C\x00\x00\x00\x02\x62\x00\x00\x01\x75\x62\x00\x00\x01\xC7\x6A" == erlang.term_to_binary(
            [373, 455]
        )
        assert b"\x83\x6C\x00\x00\x00\x01\x6A\x6A" == erlang.term_to_binary([[]])
        assert b"\x83\x6C\x00\x00\x00\x02\x6A\x6A\x6A" == erlang.term_to_binary([[], []])
        assert (
            b"\x83\x6C\x00\x00\x00\x03\x6C\x00\x00\x00\x02\x6B\x00\x04\x74\x68\x69\x73\x6B\x00\x02\x69\x73\x6A"
            b"\x6C\x00\x00\x00\x01\x6C\x00\x00\x00\x01\x6B\x00\x01\x61\x6A\x6A\x6B\x00\x04\x74\x65\x73\x74\x6A"
            == erlang.term_to_binary([["this", "is"], [["a"]], "test"])
        )

    def test_term_to_binary_list(self):
        assert b"\x83l\0\0\0\1jj" == erlang.term_to_binary([[]])
        assert b"\x83l\0\0\0\5jjjjjj" == erlang.term_to_binary([[], [], [], [], []])
        assert b"\x83l\0\0\0\5jjjjjj" == erlang.term_to_binary(
            erlang.OtpErlangList(
                [
                    erlang.OtpErlangList([]),
                    erlang.OtpErlangList([]),
                    erlang.OtpErlangList([]),
                    erlang.OtpErlangList([]),
                    erlang.OtpErlangList([]),
                ]
            )
        )

    def test_term_to_binary_improper_list(self):
        assert b"\x83l\0\0\0\1h\0h\0" == erlang.term_to_binary(erlang.OtpErlangList([(), ()], improper=True))
        assert b"\x83l\0\0\0\1a\0a\1" == erlang.term_to_binary(erlang.OtpErlangList([0, 1], improper=True))

    def test_term_to_binary_unicode(self):
        assert b"\x83j" == erlang.term_to_binary("")
        assert b"\x83k\0\4test" == erlang.term_to_binary("test")
        assert b"\x83k\0\3\x00\xc3\xbf" == erlang.term_to_binary(b"\x00\xc3\xbf")
        assert b"\x83k\0\2\xc4\x80" == erlang.term_to_binary(b"\xc4\x80")
        assert b"\x83k\0\x08\xd1\x82\xd0\xb5\xd1\x81\xd1\x82" == erlang.term_to_binary(
            b"\xd1\x82\xd0\xb5\xd1\x81\xd1\x82"
        )
        # becomes a list of small integers
        assert b"\x83l\x00\x02\x00\x00" + (b"a\xd0a\x90" * 65536) + b"j" == erlang.term_to_binary(b"\xd0\x90" * 65536)

    def test_term_to_binary_atom(self):
        assert b"\x83d\0\0" == erlang.term_to_binary(erlang.OtpErlangAtom(b""))
        assert b"\x83d\0\4test" == erlang.term_to_binary(erlang.OtpErlangAtom(b"test"))
        assert b"\x83w\x05\xc4\x80sdf" == erlang.term_to_binary(erlang.OtpErlangAtom(b"\xc4\x80sdf"))
        assert b"\x83v\x04\x00" + (b"\xc4\x80" * 512) == erlang.term_to_binary(erlang.OtpErlangAtom("\u0100" * 512))

    def test_term_to_binary_string_basic(self):
        assert b"\x83\x6A" == erlang.term_to_binary("")
        assert b"\x83\x6B\x00\x04\x74\x65\x73\x74" == erlang.term_to_binary("test")
        assert b"\x83\x6B\x00\x09\x74\x77\x6F\x20\x77\x6F\x72\x64\x73" == erlang.term_to_binary("two words")
        assert (
            b"\x83\x6B\x00\x16\x74\x65\x73\x74\x69\x6E\x67\x20\x6D\x75\x6C\x74"
            b"\x69\x70\x6C\x65\x20\x77\x6F\x72\x64\x73" == erlang.term_to_binary("testing multiple words")
        )
        assert b"\x83\x6B\x00\x01\x20" == erlang.term_to_binary(" ")
        assert b"\x83\x6B\x00\x02\x20\x20" == erlang.term_to_binary("  ")
        assert b"\x83\x6B\x00\x01\x31" == erlang.term_to_binary("1")
        assert b"\x83\x6B\x00\x02\x33\x37" == erlang.term_to_binary("37")
        assert b"\x83\x6B\x00\x07\x6F\x6E\x65\x20\x3D\x20\x31" == erlang.term_to_binary("one = 1")
        assert (
            b"\x83\x6B\x00\x20\x21\x40\x23\x24\x25\x5E\x26\x2A\x28\x29\x5F\x2B"
            b"\x2D\x3D\x5B\x5D\x7B\x7D\x5C\x7C\x3B\x27\x3A\x22\x2C\x2E\x2F\x3C"
            b"\x3E\x3F\x7E\x60" == erlang.term_to_binary("!@#$%^&*()_+-=[]{}\\|;':\",./<>?~`")
        )
        assert b"\x83\x6B\x00\x09\x22\x08\x0C\x0A\x0D\x09\x0B\x53\x12" == erlang.term_to_binary('"\b\f\n\r\t\v\123\x12')

    def test_term_to_binary_string(self):
        assert b"\x83j" == erlang.term_to_binary("")
        assert b"\x83k\0\4test" == erlang.term_to_binary("test")

    def test_term_to_binary_predefined_atom(self):
        assert b"\x83d\0\4true" == erlang.term_to_binary(True)
        assert b"\x83d\0\5false" == erlang.term_to_binary(False)
        assert b"\x83d\0\11undefined" == erlang.term_to_binary(None)

    def test_term_to_binary_short_integer(self):
        assert b"\x83a\0" == erlang.term_to_binary(0)
        assert b"\x83a\xff" == erlang.term_to_binary(255)

    def test_term_to_binary_integer(self):
        assert b"\x83b\xff\xff\xff\xff" == erlang.term_to_binary(-1)
        assert b"\x83b\x80\0\0\0" == erlang.term_to_binary(-2147483648)
        assert b"\x83b\0\0\1\0" == erlang.term_to_binary(256)
        assert b"\x83b\x7f\xff\xff\xff" == erlang.term_to_binary(2147483647)

    def test_term_to_binary_long_integer(self):
        assert b"\x83n\4\0\0\0\0\x80" == erlang.term_to_binary(2147483648)
        assert b"\x83n\4\1\1\0\0\x80" == erlang.term_to_binary(-2147483649)
        assert b"\x83o\0\0\1\0\0" + b"\0" * 255 + b"\1" == erlang.term_to_binary(2**2040)
        assert b"\x83o\0\0\1\0\1" + b"\0" * 255 + b"\1" == erlang.term_to_binary(-(2**2040))

    def test_term_to_binary_float(self):
        assert b"\x83F?\xe0\0\0\0\0\0\0" == erlang.term_to_binary(0.5)
        assert b"\x83F\xbf\xe0\0\0\0\0\0\0" == erlang.term_to_binary(-0.5)
        assert b"\x83F\0\0\0\0\0\0\0\0" == erlang.term_to_binary(0.0)
        assert b"\x83F@\t!\xfbM\x12\xd8J" == erlang.term_to_binary(3.1415926)
        assert b"\x83F\xc0\t!\xfbM\x12\xd8J" == erlang.term_to_binary(-3.1415926)

    def test_term_to_binary_compressed_term(self):
        assert b"\x83P\x00\x00\x00\x15x\x9c\xcba``\xe0\xcfB\x03\x00B@\x07\x1c" == erlang.term_to_binary(
            [[]] * 15, compressed=True
        )
        assert b"\x83P\x00\x00\x00\x15x\x9c\xcba``\xe0\xcfB\x03\x00B@\x07\x1c" == erlang.term_to_binary(
            [[]] * 15, compressed=6
        )
        assert b"\x83P\x00\x00\x00\x15x\xda\xcba``\xe0\xcfB\x03\x00B@\x07\x1c" == erlang.term_to_binary(
            [[]] * 15, compressed=9
        )
        assert (
            b"\x83P\x00\x00\x00\x15x\x01\x01\x15\x00\xea\xffl\x00\x00\x00\x0fjjjjjjjjjjjjjjjjB@\x07\x1c"
            == erlang.term_to_binary([[]] * 15, compressed=0)
        )
        assert b"\x83P\x00\x00\x00\x15x\x01\xcba``\xe0\xcfB\x03\x00B@\x07\x1c" == erlang.term_to_binary([[]] * 15, 1)
        assert b"\x83P\0\0\0\x17\x78\xda\xcb\x66\x10\x49\xc1\2\0\x5d\x60\x08\x50" == erlang.term_to_binary(
            "d" * 20, compressed=9
        )
