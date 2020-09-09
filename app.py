import os
import ssl
import sys
import time
import uuid
from urllib.parse import urlparse
import http.client

from azure.core.pipeline import Pipeline
from azure.core.pipeline.transport import HttpRequest, HttpResponse, HttpTransport, RequestsTransport

from typing import Iterator, Optional, ContextManager
import httpx

from azure.storage.blob import BlobClient
class HttpXTransportResponse(HttpResponse):
    def __init__(self,
            request: HttpRequest,
            httpx_response: httpx.Response,
            stream_contextmanager: Optional[ContextManager]=None,
        ):
        super(HttpXTransportResponse, self).__init__(request, httpx_response)
        self.status_code = httpx_response.status_code
        self.headers = httpx_response.headers
        self.reason = httpx_response.reason_phrase
        self.content_type = httpx_response.headers.get('content-type')
        self.stream_contextmanager = stream_contextmanager

    def body(self):
        return self.internal_response.content

    def stream_download(self, _) -> Iterator[bytes]:
        return HttpxStreamDownloadGenerator(_, self)

class HttpxStreamDownloadGenerator(object):
    def __init__(self, _, response):
        self.response = response
        self.iter_bytes_func = self.response.internal_response.iter_bytes()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.iter_bytes_func)
        except StopIteration:
            self.response.stream_contextmanager.__exit__()
            raise

class HttpXTransport(HttpTransport):
    def __init__(self):
        self.client = None

    def open(self):
        self.client = httpx.Client()

    def close(self):
        self.client = None

    def __enter__(self) -> "HttpXTransport":
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    def send(self, request: HttpRequest, **kwargs) -> HttpResponse:

        print(f"I was told to send a {request.method} request to {request.url}")
        stream_response = kwargs.pop("stream", False)
        parameters = {
            "method": request.method,
            "url": request.url,
            "headers": request.headers.items(),
            "data": request.data,
            "files": request.files,
            "allow_redirects": False,
            **kwargs
        }

        stream_ctx = None  # type: Optional[ContextManager]
        if stream_response:
            stream_ctx = self.client.stream(**parameters)
            response = stream_ctx.__enter__()
        else:
            response = self.client.request(**parameters)

        return HttpXTransportResponse(
            request,
            response,
            stream_contextmanager=stream_ctx,
        )

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

pipeline = Pipeline(transport=RequestsTransport())
pipelinex = Pipeline(transport=HttpXTransport())

blob_client = BlobClient.from_blob_url(url)
block_id = str(uuid.uuid4())

array = os.urandom(size)
with pipeline, pipelinex:
    while True:
        try:
            blob_client.delete_blob()
        except:
            pass

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
        req = HttpRequest("PUT", url, data=LargeStream(size), headers=headers)
        resp = pipelinex.run(req)
        stop = time.perf_counter()
        duration = stop - start
        mbps = ((size / duration) * 8) / (1024 * 1024)
        print(f'[PipelineX, stream] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.http_response.status_code}')

        start = time.perf_counter()
        req = HttpRequest("PUT", url, data=array, headers=headers)
        resp = pipelinex.run(req)
        stop = time.perf_counter()
        duration = stop - start
        mbps = ((size / duration) * 8) / (1024 * 1024)
        print(f'[PipelineX, array] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.http_response.status_code}')

        start = time.perf_counter()
        req = HttpRequest("PUT", url, data=LargeStream(size), headers=headers)
        resp = pipeline.run(req)
        stop = time.perf_counter()
        duration = stop - start
        mbps = ((size / duration) * 8) / (1024 * 1024)
        print(f'[Pipeline, stream] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.http_response.status_code}')

        start = time.perf_counter()
        req = HttpRequest("PUT", url, data=array, headers=headers)
        resp = pipeline.run(req)
        stop = time.perf_counter()
        duration = stop - start
        mbps = ((size / duration) * 8) / (1024 * 1024)
        print(f'[Pipeline, array] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps), Response={resp.http_response.status_code}')

        start = time.perf_counter()
        blob_client.stage_block(block_id, LargeStream(size), length=size)
        stop = time.perf_counter()
        duration = stop - start
        mbps = ((size / duration) * 8) / (1024 * 1024)
        print(f'[stage_block, stream] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)')


        # Calling stage_block() with an array **once** improves the perf of stage_block() with a stream for all future calls (!)

        # start = time.perf_counter()
        # blob_client.stage_block(block_id, array, length=size)
        # stop = time.perf_counter()

        # duration = stop - start
        # mbps = ((size / duration) * 8) / (1024 * 1024)

        # print(f'[stage_block, array] Put {size:,} bytes in {duration:.2f} seconds ({mbps:.2f} Mbps)')
