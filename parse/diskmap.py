from contextlib import contextmanager
import collections.abc
import typing
import pathlib

import lmdb
import cbor2
import zstandard


class DiskMap(collections.abc.MutableMapping):

    __slots__ = (
        "file_name",
        "env",
        "db",
        "db_key_map",
        "encoded_keys",
        "reversed_encoded_keys",
        "cache",
    )

    @staticmethod
    @contextmanager
    def open(file_name: typing.Union[str, pathlib.Path], readonly=False, cache=True):
        dm = DiskMap(file_name, readonly=readonly, cache=cache)
        try:
            yield dm
        finally:
            dm.close()

    @staticmethod
    def load(file_name: typing.Union[str, pathlib.Path]):
        with DiskMap.open(file_name, readonly=True, cache=False) as dm:
            return {k: v for k, v in dm.items()}

    def __init__(
        self, file_name: typing.Union[str, pathlib.Path], readonly=False, cache=True
    ):
        self.file_name: str = str(file_name)
        self.env: lmdb.Environment = lmdb.open(
            self.file_name,
            subdir=False,
            max_dbs=255,
            map_size=int(1e9),
            readonly=readonly,
        )
        self.db: lmdb.Database = self.env.open_db(b"data")
        self.db_key_map: lmdb.Database = self.env.open_db(b"key_map")
        self.encoded_keys: typing.Dict[str, bytes] = {}
        self.reversed_encoded_keys: typing.Dict[bytes, str] = {}
        self.cache = {} if cache else None
        with self.env.begin(db=self.db) as txn:
            self._read_encoded_keys(txn)

    def __del__(self):
        self.close()

    def close(self):
        if self.env:
            self.env.close()
            self.env = None
            self.db = None
            self.db_key_map = None

    def _read_encoded_keys(self, txn):
        for serialized_key, encoded_key in txn.cursor(db=self.db_key_map):
            key = cbor2.loads(serialized_key)
            self.encoded_keys[key] = encoded_key
            self.reversed_encoded_keys[encoded_key] = key

    def _next_column_id(self, txn) -> bytes:
        i = txn.stat(db=self.db_key_map)["entries"]
        if i <= 255:
            return b"\00" + i.to_bytes(1, "big")
        if i <= 65535:
            return b"\01" + (i - 255).to_bytes(2, "big")
        return b"\02" + i.to_bytes(4, "big")

    def _get_encoded_key(self, key, txn) -> bytes:
        encoded_key = self.encoded_keys.get(key)
        if encoded_key:
            return encoded_key
        serialized_key = cbor2.dumps(key)
        encoded_key = txn.get(serialized_key, db=self.db_key_map)
        if encoded_key is None:
            encoded_key = self._next_column_id(txn)
            txn.put(serialized_key, encoded_key, db=self.db_key_map)
            self.encoded_keys[key] = encoded_key
            self.reversed_encoded_keys[encoded_key] = key
        return encoded_key

    def _get_decoded_key(self, encoded_key: bytes, txn) -> typing.Any:
        key = self.reversed_encoded_keys.get(encoded_key)
        if key:
            return key
        self._read_encoded_keys(txn)
        return self.reversed_encoded_keys[encoded_key]

    def _encode_item(self, item, txn) -> typing.Dict[bytes, typing.Any]:
        return {self._get_encoded_key(k, txn): v for k, v in item.items()}

    def _decode_item(self, item: typing.Dict[bytes, typing.Any], txn) -> typing.Dict:
        return {self._get_decoded_key(k, txn): v for k, v in item.items()}

    @staticmethod
    def _compress(obj: typing.Any) -> bytes:
        return zstandard.compress(obj)

    @staticmethod
    def _decompress(buffer: bytes) -> typing.Any:
        return zstandard.decompress(buffer, max_output_size=1048576)

    def _serialize_item(self, item, txn):
        return self._compress(cbor2.dumps(self._encode_item(item, txn)))

    def _deserialize_item(self, buffer, txn):
        return self._decode_item(cbor2.loads(self._decompress(buffer)), txn)

    @contextmanager
    def _txn_begin(self, *args, **kwargs):
        with self.env.begin(*args, **kwargs, db=self.db) as txn:
            yield txn

    def store(self, item_iterator):
        with self._txn_begin(write=True) as txn:
            for key, item in item_iterator.items():
                txn.put(key.encode(), self._serialize_item(item, txn))

    def drop(self):
        with self._txn_begin(write=True) as txn:
            txn.drop(self.db)

    def get(self, key, default=None):
        if key is None:
            return None
        if self.cache and key in self.cache:
            return self.cache[key]
        with self._txn_begin() as txn:
            buffer = txn.get(key.encode())
            if buffer is None:
                return default
            item = self._deserialize_item(buffer, txn)
            if self.cache:
                self.cache[key] = item
            return item

    def __getitem__(self, key) -> typing.Dict:
        if key is None:
            raise KeyError(f"{key} not found in {repr(self)}")
        if self.cache and key in self.cache:
            return self.cache[key]
        with self._txn_begin() as txn:
            buffer = txn.get(key.encode())
            if buffer is None:
                raise KeyError(f"{key} not found in {repr(self)}")
            item = self._deserialize_item(buffer, txn)
            if self.cache:
                self.cache[key] = item
            return item

    def __setitem__(self, key, item):
        with self._txn_begin(write=True) as txn:
            txn.put(key.encode(), self._serialize_item(item, txn))
        if self.cache:
            self.cache[key] = item

    def __delitem__(self, key):
        with self._txn_begin(write=True) as txn:
            if not txn.delete(key.encode()):
                raise KeyError(f"{key} not found in {repr(self)}")
            if self.cache and key in self.cache:
                del self.cache[key]

    def __len__(self):
        with self._txn_begin() as txn:
            return txn.stat(db=self.db)["entries"]

    def __iter__(self):
        return self.keys()

    def __next__(self):
        return self.items()

    def items(self):
        with self._txn_begin() as txn:
            for key, buffer in txn.cursor():
                yield key.decode(), self._deserialize_item(buffer, txn)

    def values(self):
        with self._txn_begin() as txn:
            for _, buffer in txn.cursor():
                yield self._deserialize_item(buffer, txn)

    def keys(self):
        with self._txn_begin() as txn:
            for key in txn.cursor().iternext(values=False):
                yield key.decode()

    def __repr__(self):
        return "<" + self.__class__.__name__ + ' file_name="' + self.file_name + '">'


if __name__ == "__main__":
    import sys
    import json

    file_name = sys.argv[1]
    key = sys.argv[2] if len(sys.argv) > 2 else None

    def dvalue(value):
        return json.dumps(value, indent=4)

    with DiskMap.open(sys.argv[1]) as m:
        if key:
            print(dvalue(m[key]))
        else:
            print("{")
            for k, v in m.items():
                print(f"{repr(k)}: {dvalue(v)},")
            print("}")
