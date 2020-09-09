import os
import ssl
import sys
import time
import uuid
from urllib.parse import urlparse
import http.client

from azure.storage.blob import BlobClient

class LargeStream:
    def __init__(self, length, initial_buffer_length=1024*1024):
        self._base_data = os.urandom(initial_buffer_length)
        self._base_data_length = initial_buffer_length
        self._position = 0
        self._remaining = length

    def read(self, size=None):
        if self._remaining == 0:
            return None

        if size is None:
            e = self._base_data_length
        else:
            e = size
        e = min(e, self._remaining)
        if e > self._base_data_length:
            self._base_data = os.urandom(e)
            self._base_data_length = e
        self._remaining = self._remaining - e
        return self._base_data[:e]

    def remaining(self):
        return self._remaining

if len(sys.argv) == 1:
    print('Usage: app <url> <size>')
    exit(1)

url = sys.argv[1]
size = int(sys.argv[2]) if len(sys.argv) >= 3 else 1024

print('=== Parameters ===')
print(f'Url: {url}')
print(f'Size: {size}')
print()

parsedUrl = urlparse(url)

headers = {
    "Content-Length": str(size),
    "x-ms-blob-type": "BlockBlob"
}

conn = http.client.HTTPSConnection(parsedUrl.netloc)

blob_client = BlobClient.from_blob_url(url)
block_id = str(uuid.uuid4())

array = os.urandom(size)
while True:
    start = time.perf_counter()
    conn.request("PUT", url, body=LargeStream(size), headers=headers)
    resp = conn.getresponse()
    resp.read()
    stop = time.perf_counter()

    duration = stop - start
    mbps = ((size / duration) * 8) / (1024 * 1024)

    print(f'[http.client, stream] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.status}')


    start = time.perf_counter()
    conn.request("PUT", url, body=array, headers=headers)
    resp = conn.getresponse()
    resp.read()
    stop = time.perf_counter()

    duration = stop - start
    mbps = ((size / duration) * 8) / (1024 * 1024)

    print(f'[http.client, array] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.status}')


    start = time.perf_counter()
    blob_client.stage_block(block_id, LargeStream(size), length=size)
    stop = time.perf_counter()

    duration = stop - start
    mbps = ((size / duration) * 8) / (1024 * 1024)

    print(f'[stage_block, stream] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.status}')


    start = time.perf_counter()
    blob_client.stage_block(block_id, array, length=size)
    stop = time.perf_counter()

    duration = stop - start
    mbps = ((size / duration) * 8) / (1024 * 1024)

    print(f'[stage_block, array] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.status}')
