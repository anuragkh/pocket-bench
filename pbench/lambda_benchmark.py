from __future__ import print_function

import argparse
import datetime
import errno
import json
import multiprocessing
import os
import select
import socket
import sys
import time
from multiprocessing import Process

import boto3
from botocore.exceptions import ClientError

import pocket
from handler import lambda_handler, bytes_to_str, b

iam_client = boto3.client('iam')
lambda_client = boto3.client('lambda')
function_name = 'StorageBenchmark'
job_ids = {}

DEFAULT_NUM_OPS = 50000


def create_function(name):
    env = dict()
    lambda_zip = '/tmp/build/lambda.zip'
    if not os.path.isfile(lambda_zip):
        code_path = os.path.dirname(os.path.realpath(__file__))
        num_cpu = multiprocessing.cpu_count()
        os.system('mkdir -p /tmp/build && cd /tmp/build && cmake {} && make -j {} pkg'.format(code_path, num_cpu))
    with open(lambda_zip, 'rb') as f:
        zipped_code = f.read()
    role = iam_client.get_role(RoleName='aws-lambda-execute')
    resp = lambda_client.create_function(
        FunctionName=name,
        Description='Storage Benchmark',
        Runtime='python2.7',
        Role=role['Role']['Arn'],
        Handler='benchmark_handler.benchmark_handler',
        Code=dict(ZipFile=zipped_code),
        Timeout=300,
        MemorySize=3008,
        Environment=dict(Variables=env),
    )
    print('Created function: {}'.format(resp))


def register_pocket_job(job_name="job-0"):
    if job_name not in job_ids:
        job_ids[job_name] = pocket.register_job(job_name, capacityGB=10, peakMbps=8000)
    return job_ids[job_name]


def deregister_pocket_jobs():
    for job in job_ids:
        pocket.deregister_job(job_ids[job])


def invoke_lambda(e):
    lambda_client.invoke(FunctionName=function_name, InvocationType='Event', Payload=json.dumps(e))


def invoke_locally(e):
    f = Process(target=lambda_handler, args=(e, None,))
    f.start()
    return f


def invoke(args, mode, batch_id=str(0), lambda_id=str(0)):
    e = dict(
        host=args.host,
        port=args.port,
        object_size=args.obj_size,
        num_ops=args.num_ops,
        mode=mode,
        batch_id=batch_id,
        lambda_id=lambda_id
    )
    if args.invoke:
        return invoke_lambda(e)
    elif args.invoke_local:
        return invoke_locally(e)


def invoke_n(args, mode, n, batch_size=1):
    return [invoke(args, mode, register_pocket_job("job-%s" % (i / batch_size)), str(i)) for i in range(n)]


def is_socket_valid(socket_instance):
    """ Return True if this socket is connected. """
    if not socket_instance:
        return False

    try:
        socket_instance.getsockname()
    except socket.error as err:
        err_type = err.args[0]
        if err_type == errno.EBADF:
            return False

    try:
        socket_instance.getpeername()
    except socket.error as err:
        err_type = err.args[0]
        if err_type in [errno.EBADF, errno.ENOTCONN]:
            return False

    return True


def run_server(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setblocking(False)
    s.settimeout(300)
    try:
        s.bind((host, port))
    except socket.error as ex:
        print('Bind failed: {}'.format(ex))
        sys.exit()
    s.listen(5)
    return s


def print_logs(r, msg):
    for line in msg.splitlines():
        print('** Function @ {} {} {}'.format(r.getpeername(), datetime.datetime.now(), line))


def log_worker(s, num_connections=1, log=True):
    inputs = [s]
    outputs = []
    n_closed = 0
    while inputs:
        readable, writable, exceptional = select.select(inputs, outputs, inputs)
        for r in readable:
            if r is s:
                sock, address = r.accept()
                sock.setblocking(False)
                inputs.append(sock)
            else:
                data = r.recv(4096)
                msg = bytes_to_str(data.rstrip().lstrip())
                if 'CLOSE' in msg or not data:
                    print_logs(r, msg.replace('CLOSE', ''))
                    inputs.remove(r)
                    r.close()
                    n_closed += 1
                    if n_closed == num_connections:
                        inputs.remove(s)
                        s.close()
                else:
                    if log:
                        print_logs(r, msg)


def control_worker(s, batch_size=1, num_batches=1, batch_delay=0, log=True):
    inputs = [s]
    outputs = []
    ready = []
    ids = set()
    run = True
    while run:
        readable, writable, exceptional = select.select(inputs, outputs, inputs)
        for r in readable:
            if r is s:
                sock, address = r.accept()
                sock.setblocking(False)
                inputs.append(sock)
            else:
                data = r.recv(4096)
                msg = bytes_to_str(data.rstrip().lstrip())
                if not data:
                    inputs.remove(r)
                    r.close()
                else:
                    print('DEBUG: [{}]'.format(msg))
                    i = int(msg.split('READY:')[1])
                    if log:
                        print('... Function id={} ready ...'.format(i))
                    if i not in ids:
                        if log:
                            print('... Queuing function id={} ...'.format(i))
                        ids.add(i)
                        ready.append((i, r))
                        if len(ids) == batch_size * num_batches:
                            run = False
                        else:
                            print('.. Progress {}/{}'.format(len(ids), batch_size * num_batches))
                    else:
                        if log:
                            print('... Aborting function id={} ...'.format(i))
                        r.send(b('ABORT'))
                        inputs.remove(r)
                        r.close()

    print('.. Starting benchmark ..')
    ready.sort(key=lambda x: x[0])
    for t in range(num_batches):
        for idx in range(t * batch_size, (t + 1) * batch_size):
            i, sock = ready[idx]
            if log:
                print('... Running function id={} ...'.format(i))
            sock.send(b('RUN'))
        if log:
            print('.. End of wave ..')
            print('.. Sleeping for {}s ..'.format(batch_delay))
        time.sleep(batch_delay)
    s.close()


def log_process(host, port, num_loggers=1, log=True):
    s = run_server(host, port)
    if log:
        print('... Log server listening on {}:{} ...'.format(host, port))
    p = Process(target=log_worker, args=(s, num_loggers, log))
    p.start()
    return p


def control_process(host, port, batch_size=1, num_batches=1, batch_delay=0, log=True):
    s = run_server(host, port)
    if log:
        print('... Control server listening on {}:{} ...'.format(host, port))
    p = Process(target=control_worker, args=(s, batch_size, num_batches, batch_delay, log))
    p.start()
    return p


def main():
    m_help = ('\n\nmode should contain one or more of the following components,\n'
              'separated by any non-whitespace delimiter:\n'
              '\tread     - execute read benchmark\n'
              '\twrite    - execute write benchmark\n'
              'Examples:\n'
              '\twrite_read - execute write and read benchmarks, in that order.')
    parser = argparse.ArgumentParser(description='Run pocket benchmark on AWS Lambda.',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--create', action='store_true', help='create AWS Lambda function')
    parser.add_argument('--invoke', action='store_true', help='invoke AWS Lambda function')
    parser.add_argument('--invoke-local', action='store_true', help='invoke AWS Lambda function locally')
    parser.add_argument('--quiet', action='store_true', help='Suppress function logs')
    parser.add_argument('--quieter', action='store_true', help='Suppress all logs')
    parser.add_argument('--host', type=str, default=socket.gethostname(), help='name of host where script is run')
    parser.add_argument('--port', type=int, default=8888, help='port that server listens on')
    parser.add_argument('--num-ops', type=int, default=DEFAULT_NUM_OPS, help='number of operations')
    parser.add_argument('--obj-size', type=int, default=8, help='object size to benchmark for')
    parser.add_argument('--mode', type=str, default='write_read', help='benchmark mode' + m_help)
    args = parser.parse_args()

    if args.create:
        try:
            create_function(function_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print('Skipping function creation since it already exists...')
            else:
                print('Unexpected error: {}'.format(e))
                raise
        print('Creation successful!')

    if args.invoke or args.invoke_local:
        log_function = not args.quiet and not args.quieter
        log_control = not args.quieter
        host = args.host
        log_port = args.port
        control_port = log_port + 1
        if args.mode.startswith('scale'):
            _, mode, batch_size, batch_delay, num_batches = args.mode.split(':')
            batch_size = int(batch_size)
            batch_delay = int(batch_delay)
            num_batches = int(num_batches)
            num_functions = batch_size * num_batches
            print('.. Number of functions to launch = {} ..'.format(num_functions))
            lp = log_process(host, log_port, num_functions, log_function)
            op = control_process(host, control_port, batch_size, num_batches, batch_delay, log_control)
            processes = invoke_n(args, mode, num_functions, batch_size)
            processes.append(lp)
            processes.append(op)
        else:
            lp = log_process(host, log_port)
            op = control_process(host, control_port)
            processes = [invoke(args, args.mode, register_pocket_job("job-0")), lp, op]

        for p in processes:
            if p is not None:
                p.join()
                print('... {} terminated ...'.format(p))

        deregister_pocket_jobs()


if __name__ == '__main__':
    main()
