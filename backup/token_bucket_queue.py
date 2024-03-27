#!/usr/bin/python3

 # Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 #
 # Licensed under the Apache License, Version 2.0 (the "License"). You
 # may not use this file except in compliance with the License. A copy of
 # the License is located at
 #
 # http://aws.amazon.com/apache2.0/
 #
 # or in the "license" file accompanying this file. This file is
 # distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
 # ANY KIND, either express or implied. See the License for the specific
 # language governing permissions and limitations under the License.

"""
This module provides a convenient interface to download files from S3 concurrently. That is multiple parts
of single file are downloaded concurrently thereby achieving much higher transfer rates over using a single threaded
download.

A token bucket is used to optionally limit download bandwidth. A similar algorithm can be created to limit upload
bandwidth as well

This module can achieve average transfer rates of 1,600 megabits per second (mbps)

Python 3.4+ is required

Example:

import boto

BUCKET = "YOUR_BUCKET"
KEY = "YOUR_KEY"
destination_file = "YOUR_LOCAL_FILE_DESTINATION"
MB = 1024 ** 2

s3_conn = boto.connect_s3()
key = s3_conn.get_bucket(BUCKET).get_key(KEY)
cd = ConcurrentDownloader(key, s3_conn, max_rate_Bps=12 * MB)
cd.download_file(destination_file)

"""

from queue import Queue
from threading import Lock
from time import time, sleep
import os
import math
import logging
from multiprocessing.dummy import Pool
from datetime import datetime
import itertools

# Funcy is used to easily handle retry logic
import funcy

MB = 1024 ** 2

log = logging.getLogger("{}.{}".format("main", __name__))


class _BWQueue(Queue):
    _token_bucket = None

    def __init__(self, part_size_B, max_rate_Bps, maxsize=0):
        """
        Implements a Queue that can be used to limit bandwidth. The Queue only allows a maximum
        configurable number of gets per second. This limitation is used to limit bandwidth
        utilization.

        :param part_size_B:    Part size in Bytes
        :param max_rate_Bps:   Maximum bandwidth in Bytes per Second
        :param maxsize:        Maximum Queue Size
        :return:
        """
        parts_per_s = self._get_parts_per_second(part_size_B, max_rate_Bps)
        #TODO: May want a burst rate -- change the 1
        log.info("Parts per Sec: {}".format(parts_per_s))
        self._token_bucket = _TokenBucket(1, parts_per_s)
        super(_BWQueue, self).__init__(maxsize=maxsize)

    def _get_parts_per_second(self, part_size_B, max_rate_Bps):
        """
        Get the parts per second that sets the rate for the Token Bucket

        :param part_size_B:
        :param max_rate_Bps:
        :return:
        """
        parts_per_s = (max_rate_Bps / part_size_B)
        return parts_per_s

    def get(self, block=True, timeout=None):
        while True:
            # This step makes sure that all threads are coordinated based
            # on tokens in a token bucket. Tokens are a proxy for parts, .e.g one token equals
            # one part. Tokens are handed out at a consistent rate across all threads

            # Try to consume one token from the bucket
            ct = self._token_bucket.consume(1)
            log.debug("Tokens Available: {}".format(self._token_bucket.tokens))
            # If a token is available, then get the next item from the queue
            if ct:
                log.debug("OK to get chunk from queue...")
                return super(_BWQueue, self).get(block=block, timeout=timeout)
            # A token is not available, wait and then try again.
            sleep(.1)


class _TokenBucket(object):
    """
    An implementation of the token bucket algorithm.

    >>> bucket = _TokenBucket(80, 0.5)
    >>> print bucket.consume(10)
    True
    >>> print bucket.consume(90)
    False
    """
    def __init__(self, tokens, fill_rate):
        """

        :param tokens:      The total tokens in the bucket
        :param fill_rate:   Rate in tokens/second that the bucket will be refilled
        :return:
        """

        self.capacity = tokens
        self._tokens = tokens
        self.fill_rate = fill_rate
        self.timestamp = time()
        self.lock = Lock()

    def consume(self, tokens):
        """
        Consume tokens from the bucket

        :param tokens:  Number of tokens to consume
        :return:        True if there were sufficient tokens otherwise False.
        """
        # If the bucket has enough tokens available to meet the request
        if tokens <= self.tokens:
            with self.lock:
                # Reduce the tokens in the bucket
                self._tokens -= tokens
            return True
        else:
            return False


    def _get_tokens(self):
        """
        Get the number of tokens in bucket

        :return:    Number of Tokens
        """
        if self._tokens <= self.capacity:
            now = time()
            # Calculate how many tokens to add into the bucket
            delta = self.fill_rate * (now - self.timestamp)
            with self.lock:
                # Add more tokens into the bucket
                self._tokens = min(self.capacity, self._tokens + delta)
                self.timestamp = now
        return self._tokens

    tokens = property(_get_tokens)


class ConcurrentDownloader:
    _retry_download_timeout = 3     # Time between retries
    _num_retries = 3                # Number of retries
    _key = None
    _num_threads = None
    _part_size = None
    _s3_conn = None
    _bw_queue = None

    def __init__(self, key, s3_conn, num_threads=10, part_size=64*MB, max_rate_Bps=None):
        """
        Testing has shown that 10 threads and 64MB part size give the best overall throughput

        :param key: An BOTO S3 Key Object, e.g. key = s3_conn.get_bucket('YOUR_BUCKET').get_key('YOUR_KEY')
        :param s3_conn: A BOTO S3 connection, e.g. s3_conn = boto.connect_s3()
        :param num_threads: Number of concurrent threads
        :param min_size_for_multipart_download: Minimum size of key in bytes for a concurrent download
        :param part_size: Size of each downloaded chunk
        :param max_rate_Bps: Maximum bandwidth in Bytes per Second
        :return:
        """
        if part_size < 1 * MB:
            raise ValueError("Minimum Part Size is 1MB: {}".format(part_size))
        if not key.exists():
            raise ValueError("Key not found: {}".format(key))
        # Test that num_threads is an int. 3.5 threads makes no sense
        if not isinstance(num_threads, int):
            raise ValueError("num_threads must be an integer: {}".format(num_threads))
        if num_threads < 1:
            raise ValueError("num_threads must be at least 1: {}".format(num_threads))
        self._key = key
        self._s3_conn = s3_conn
        self._num_threads = num_threads
        self._part_size = part_size
        # If we need to limit bandwidth, implement a bandwidth limiting queue
        if max_rate_Bps:
            self._bw_queue = _BWQueue(part_size_B=part_size, max_rate_Bps=max_rate_Bps)

    def download_file(self, destination_file, overwrite=False):
        """
        Download file from S3
        :param destination_file:    Destination file to download to
        :param overwrite:           Boolean value whether or not to overwrite an existing destination_file
        :return:
        """
        start_time = datetime.utcnow()
        self._destination_file = destination_file
        if os.path.exists(self._destination_file) and overwrite is False:
            raise ValueError("Destination file already exists. {}".format(self._destination_file))
        if os.path.exists(self._destination_file) and overwrite:
            log.debug("Deleting destination file: {}".format(self._destination_file))
            os.remove(self._destination_file)
        self._concurrent_download()
        # Use max to handle division by 0 errors
        elapsed_time = max((datetime.utcnow() - start_time).seconds, .1)
        rate = self._get_xfer_rate(elapsed_time)
        log.debug("Download Complete. Elapsed Time (s): {0:.2f} Rate (MBps): {0:.2f}".format(elapsed_time, rate))

    def _create_empty_file(self, file_path):
        """
        Creates a blank file of 0 bytes

        :param file_path:
        :return:
        """
        log.debug("Creating empty file: {}".format(file_path))
        open(file_path, 'w').close()

    def _get_xfer_rate(self, elapsed_time):
        """
        Get transfer rate in MBps
        :param elapsed_time:
        :return:
        """
        rate = self._key.size / MB / elapsed_time
        return rate

    def _concurrent_download(self):
        """
        Perform a concurrent download
        :return:
        """
        log.debug("Starting Concurrent download")
        self._create_empty_file(self._destination_file)
        # Split two generators one for the part downloads and one for the bandwidth queue
        part_ranges, dummy_part_ranges = itertools.tee(self._get_part_ranges())
        # Fill a queue construct that will be used to artificially limit download speed
        if self._bw_queue:
            # Part counters are put into a queue
            for i in dummy_part_ranges:
                self._bw_queue.put(i)
        pool = Pool(processes=self._num_threads)
        # Using starmap unpacks the tuple returned by _get_part_ranges
        # Using starmap will run the downloads concurrently and wait at this step until all downloads complete
        pool.starmap(func=self._download_part, iterable=part_ranges)
        log.debug("Completed Concurrent download")

    def _get_num_parts(self):
        """
        Get the number of parts in a concurrent download
        :return:
        """
        # We want to round up partial parts to full parts, i.e. there can't be a 2.5 part download. Use the
        # ceil function to round up to the next integer
        num_parts = math.ceil(self._key.size / self._part_size)
        log.debug("Number of Parts: {}".format(num_parts))
        return num_parts

    def _get_part_ranges(self):
        """
        Generator to get the start_byte, end_byte tuples for each download part
        :return:
        """
        file_size = self._key.size
        for i in range(self._get_num_parts()):
            start_byte = self._part_size * i
            # The last part may not be a full part and hence the min function
            # Byte ranges start at 0 hence the -1 offset
            end_byte = min(start_byte + self._part_size - 1, file_size - 1)
            log.debug("Part Range: start_byte: {} end_byte: {}".format(start_byte, end_byte))
            yield (start_byte, end_byte)

    @funcy.retry(_num_retries, timeout=3)
    def _download_part(self, start_byte, end_byte):
        """
        Download an individual part of the key from S3. Save that part to a specific offset in the file

        :param start_byte:
        :param end_byte:
        :return:
        """
        log.debug("Downloading Part. start_byte: {} end_byte: {}".format(start_byte, end_byte))
        start_time = datetime.utcnow()
        # This causes a delay
        if self._bw_queue:
            # This get will block if downloader will exceed bandwidth utilization
            # The part counters that we get from the queue have no value so they are stored in _dummy_var
            # for clarity
            _dummy_var = self._bw_queue.get()
        part_num = int(start_byte / self._part_size) + 1
        range_string = 'bytes={}-{}'.format(start_byte, end_byte)      # create byte range as string
        range_header = {'Range': range_string}
        # Attempted to use the following elegant code to get the byte ranges from S3
        # Unfortunately BOTO is not thread safe. Different connections to S3 are stepping on each other
        # and corrupting the data coming down. Locking around the download defeats the multithreaded download
        # data = self._key.get_contents_as_string(headers=range_header, encoding=None)

        # Make range request to S3 for part
        log.info("Download Part: {}".format(part_num))
        response = self._s3_conn.make_request("GET", bucket=self._key.bucket.name, key=self._key.name,
                                              headers=range_header)
        data = response.read()

        f = None  # File pointer for destination file
        try:
            # Open destination file in write mode
            f = os.open(self._destination_file, os.O_WRONLY)
            # Seek to location in file
            os.lseek(f, start_byte, os.SEEK_SET)
            log.debug("Writing downloaded data to file...")
            os.write(f, data)
        finally:
            if f:
                log.debug("Closing file...")
                os.close(f)
        # Use max to avoid division by zero errors
        elapsed_time = max((datetime.utcnow() - start_time).seconds, .1)
        rate = self._get_xfer_rate(elapsed_time)
        log.debug("Downloaded Part. Start_byte: {0:d} End_Byte: "
                  "{0:d} Elapsed Time (s): {0:.2f} Rate (MBps): {0:.2f}".format(start_byte, end_byte,
                                                                                elapsed_time, rate))
