from __future__ import print_function

import os
import socket
import sys
import time

import boto3

import pocket

if sys.version_info[0] < 3:
    def b(x):
        return x


    def bytes_to_str(x):
        return x
else:
    def b(x):
        return x.encode('utf-8') if not isinstance(x, bytes) else x


    def bytes_to_str(x):
        return x.decode('utf-8') if isinstance(x, bytes) else x


class Logger(object):
    def __init__(self, f):
        self.f = f

    def info(self, msg):
        self._log('INFO', msg)

    def warn(self, msg):
        self._log('WARN', msg)

    def error(self, msg):
        self._log('ERROR', msg)

    def _log(self, msg_type, msg):
        self.f.send(b('{} {}'.format(msg_type, msg).rstrip()))

    def close(self):
        self.f.send(b('CLOSE'))
        self.f.shutdown(socket.SHUT_RDWR)
        self.f.close()

    def abort(self, msg):
        self.f.send('ABORT:{}'.format(msg))
        self.f.shutdown(socket.SHUT_RDWR)
        self.f.close()


def _connect_logger(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    return Logger(sock)


def _signal(host, port, lambda_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(b("READY:" + lambda_id))
    return bytes_to_str(sock.recv(4096))


def _copy_results(logger, result):
    bucket = boto3.resource('s3').Bucket('bench-results')
    if os.path.isfile(result):
        logger.info('Copying results @ {} to S3...'.format(result))
        with open(result, 'rb') as data:
            bucket.put_object(Key=os.path.join("pocket", os.path.basename(result)), Body=data)
    else:
        logger.warn('Result file {} not found'.format(result))


def _pocket_write_buffer(p, job_id, lambda_id, num_ops, src, size):
    with open('pocket_write_%s.txt' % lambda_id, 'w') as output:
        for i in xrange(num_ops):
            dst_filename = 'tmp-' + lambda_id + '-' + str(i)
            t0 = time.time()
            r = pocket.put_buffer(p, src, size, dst_filename, job_id)
            t1 = time.time()
            tt = (t1 - t0) * 1e6
            output.write('%d\n' % tt)
            if r != 0:
                raise Exception("put buffer failed: " + dst_filename)


def _pocket_read_buffer(p, job_id, lambda_id, num_ops, size):
    with open('pocket_read_%s.txt' % lambda_id, 'w') as output:
        text_back = " " * size
        for i in xrange(num_ops):
            dst_filename = 'tmp-' + lambda_id + '-' + str(i)
            t0 = time.time()
            r = pocket.get_buffer(p, dst_filename, text_back, size, job_id)
            t1 = time.time()
            tt = (t1 - t0) * 1e6
            output.write('%d\n' % tt)
            if r != 0:
                raise Exception("get buffer failed: " + dst_filename)


def lambda_handler(event, context):
    # Read event info
    host = event.get('host')
    log_port = int(event.get('port'))
    control_port = log_port + 1
    mode = event.get('mode')
    lambda_id = event.get('lambda_id')
    job_id = event.get('batch_id')
    object_size = event.get('object_size')
    num_ops = event.get('num_ops')
    namenode_ip = "10.1.0.10"
    namenode_port = 9070
    text = 'a' * object_size

    try:
        logger = _connect_logger(host, log_port)
    except Exception as e:
        print('Exception: {}'.format(e))
        raise

    logger.info('Event: {}, Context: {}'.format(event, context))

    response = _signal(host, control_port, lambda_id)
    if response != "RUN":
        logger.abort("Control returned " + response)
        return

    # connect to pocket
    p = pocket.connect(namenode_ip, namenode_port)

    if "write" in mode:
        _pocket_write_buffer(p, job_id, lambda_id, num_ops, text, object_size)

    if "read" in mode:
        _pocket_read_buffer(p, job_id, lambda_id, num_ops, object_size)

    # pocket.close(p)

    _copy_results(logger, "pocket_write_%s.txt" % lambda_id)
    _copy_results(logger, "pocket_read_%s.txt" % lambda_id)
    logger.close()
    return
