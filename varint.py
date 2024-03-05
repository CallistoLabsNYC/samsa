from typing import Iterable
from argparse import ArgumentParser
from sys import exit

parser = ArgumentParser(description="Parses and dencodes protobuf varints")
parser.add_argument("mode", help="Whether to encode or decode")
parser.add_argument("num", help="The number to decode/encode")

args = parser.parse_args()

def decodeVarint(source: Iterable[int]):
    result = 0
    numBytes = 0
    for read in source:
        result |= ((read & 0x7F) << (numBytes * 7))
        numBytes += 1
        if (read & 0x80) != 0x80:
            return result
    raise ValueError("Expected more bytes for: " + result)

def encodeVarint(num) -> bytes:
    result = bytearray()
    while (num & ~0x7F) != 0:
        # We still have more than '0x7F' bits
        result.append((num & 0x7F) | 0x80)
        num >>= 7
    result.append(num)
    return bytes(result)


if args.mode == "encode":
    num = int(args.num)
    print(num, "as a varint:", encodeVarint(num).hex().upper())
elif args.mode == "decode":
    varint_bytes = bytes.fromhex(args.num)
    try:
        value = decodeVarint(varint_bytes)
        print("Value of '{}': {}".format(
            varint_bytes.hex().upper(),
            value
        ))
    except ValueError as e:
        print("Invalid varint '{}': {}".format(
            varint_bytes.hex().upper(),
            str(e)
        ))
        exit(1)
else:
    print("Invalid mode:", args.mode)
    exit(1)
