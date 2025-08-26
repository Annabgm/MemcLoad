#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import threading
import queue
import time
from itertools import islice
from optparse import OptionParser
import collections
import multiprocessing as mp
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
from pymemcache.client.base import Client
from pymemcache.client.retrying import RetryingClient
from pymemcache.exceptions import MemcacheUnexpectedCloseError


NORMAL_ERR_RATE = 0.01
CHUNK_SIZE = 128
MEMCACHE_SOCKET_TIMEOUT = 2
RETRY_DELAY = 0.01
RETRY_ATTEMPTS = 3
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def insert_appsinstalled(memc_addr, appsinstalled, options):
    logger = create_logger(options)
    elements = {}
    elements_ua = {}
    dev_type = appsinstalled[0].dev_type
    for ap in appsinstalled:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = ap.lat
        ua.lon = ap.lon
        key = "%s:%s" % (ap.dev_type, ap.dev_id)
        ua.apps.extend(ap.apps)
        packed = ua.SerializeToString()
        elements[key] = packed
        elements_ua[key] = ua
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if options.dry:
            for el_k, el_v in elements_ua.items():
                logger.debug("%s - %s -> %s" % (memc_addr, el_k, str(el_v).replace("\n", " ")))
        else:
            memc_addr.set_many(elements)
    except Exception as e:
        logger.exception("Cannot write to memc %s: %s" % (dev_type, e))
        return False
    return True


def parse_appsinstalled(line, options):
    logger = create_logger(options)
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logger.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logger.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class Worker(threading.Thread):
    def __init__(self, queue_load, queue_data, device_memc):
        threading.Thread.__init__(self)
        self.queue_load = queue_load
        self.queue_data = queue_data
        self.memc_dict = device_memc

    def run(self):
        while True:
            content = self.queue_load.get()
            if isinstance(content, str) and content == 'quit':
                self.queue_load.task_done()
                break

            lines, device_dict, options = content
            err, suc, devices = self.parse_lines(lines, device_dict, options)
            if not suc:
                res_req = (err, 0)
            else:
                success = 0
                fail = err
                for d, aps in devices.items():
                    # app, adr = d
                    ok = insert_appsinstalled(device_dict[d], aps, options)
                    success += len(aps)
                    fail = fail + (1 - ok) * len(aps)
                res_req = (fail, success)
            self.queue_data.put(res_req)
            self.queue_load.task_done()

    def parse_lines(self, lines, device_memc, options):
        errors = 0
        apps_dict = collections.defaultdict(list)
        success = 0
        for ln in lines:
            line = ln.decode('utf-8').strip()
            appsinstalled = parse_appsinstalled(line, options)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logger.error("Unknown device type: %s" % appsinstalled.dev_type)
                continue
            apps_dict[appsinstalled.dev_type].append(appsinstalled)
            success += 1
        return errors, success, apps_dict


def create_cliens_pool(memc_dict):
    memc_clients = {}
    for k, v in memc_dict.items():
        base_client = Client(v, connect_timeout=2., timeout=0.5)
        client = RetryingClient(
            base_client,
            attempts=RETRY_ATTEMPTS,
            retry_delay=RETRY_DELAY,
            retry_for=[MemcacheUnexpectedCloseError]
        )
        memc_clients[k] = client
    return memc_clients

def process_file(argms):
    options, device_memc, file = argms
    logger = create_logger(options)
    memc_clients = create_cliens_pool(device_memc)

    queue_load = queue.Queue()
    queue_data = queue.Queue()
    workers = []
    for _ in range(options.num_workers):
        worker = Worker(queue_load, queue_data, memc_clients)
        worker.daemon = True
        worker.start()
        workers.append(worker)

    s = time.time()
    with gzip.open(file) as fl:
        lns = list(islice(fl, CHUNK_SIZE))
        queue_load.put((lns, memc_clients, options))
    for _ in workers:
        queue_load.put('quit')

    queue_load.join()

    processed = errors = 0
    while not queue_data.empty():
        r = queue_data.get()
        processed += r[1]
        errors += r[0]
    # print('processed number {} errors number {}'.format(processed, errors))
    if processed:
        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logger.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logger.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))

    for w in workers:
        w.join()
    logger.info("%s processed in %s sec" % (file, time.time() - s))
    head, fn = os.path.split(file)
    os.rename(file, os.path.join(head, "." + fn))
    logger.info("%s been renamed to %s.%s" % (file, head, fn))


def main(options):
    logger = create_logger(options)
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    files_list = []
    for fn in glob.iglob(options.pattern):
        files_list.append((options, device_memc, fn))
    files_list = sorted(files_list, key=lambda x: x[-1])
    with mp.get_context('spawn').Pool(os.cpu_count()) as p:
        p.map(process_file, files_list)
        # p.map(dot_rename, files_list)
    logger.info('Process {} finished'.format(mp.current_process().name))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


def create_logger(opt):
    logger = mp.get_logger()
    if not opt.dry:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[%(asctime)s| %(levelname).1s| %(processName)s] %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S')
    handler = logging.FileHandler(opt.log)
    handler.setFormatter(formatter)

    # this bit will make sure you won't have
    # duplicated messages in the output
    if not len(logger.handlers):
        logger.addHandler(handler)
    return logger


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--num_workers", action="store", type=int, default=4)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:11211")
    op.add_option("--gaid", action="store", default="127.0.0.1:11212")
    op.add_option("--adid", action="store", default="127.0.0.1:11213")
    op.add_option("--dvid", action="store", default="127.0.0.1:11214")
    (opts, args) = op.parse_args()
    logger = create_logger(opts)
    if opts.test:
        prototest()
        sys.exit(0)

    logger.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logger.exception("Unexpected error: %s" % e)
        sys.exit(1)
