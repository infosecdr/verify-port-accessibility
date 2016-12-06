#!/usr/bin/env python

from contextlib import contextmanager
import csv
import StringIO
from argparse import ArgumentParser
from warnings import warn
from fabric.context_managers import hide, show
from fabric.exceptions import NetworkError, CommandTimeout
from fabric.operations import run
from fabric.state import env
from fabric.tasks import execute
import time
import subprocess
from socket import error as socket_error
import signal
import sys


class TimeoutException(Exception): pass

# context manager that puts a time limit on how long code inside can run for
# based on http://stackoverflow.com/a/601168/3753673
@contextmanager
def time_limit(seconds):
    def alarm_signal_handler(signum, frame):
        # if we got here, the specified number of seconds was exceeded
        raise TimeoutException, "Python code ran for too long"
    signal.signal(signal.SIGALRM, alarm_signal_handler) # register the alarm signal handler above
    signal.alarm(seconds) # start the timer
    try:
        yield # hand control back over to code within the context manager
    finally:
        signal.alarm(0) # turn off alarm



# split out lines from stdout but remove noise lines such as "id: cannot find name for group ID 1010"
def stdout_lines_without_noise(stdouterr_text):
    lines = []
    for line in StringIO.StringIO(stdouterr_text):
        line = line.rstrip()
        if ("id: cannot find" not in line):  # we exclude lines with "id: cannot find"
            lines.append(line)
    return lines

def first_str_match_in_stdout_lines(stdouterr_text,str):
    """finds the first stdout lines match matches a regex and returns the boolean result of the str match or None if
     no lines matched"""
    lines = stdout_lines_without_noise(stdouterr_text)
    for line in lines:
        line = line.rstrip()
        if line == str:
            return True
    # no matches
    return False



""" use fabric to test network connectivity from a remote machine to a given IP address and port; returns whether it
 succeeded or not (or raises and exception if something went wrong) """
def verify_via_fabric_source_can_connect_to_port(ip_addr, port):
    if ip_addr[0:5] == 'fe80:':
        print "skipping testing with link-scope IPv6 address %s since we don't have logic to find interface it would " \
              "be associated with" % (ip_addr)
        # TODO: add this support; need to add (e.g., "%eth4" on end of address;
        #       see https://bugzilla.redhat.com/show_bug.cgi?id=136852)
        return

    # use nc with short idle timeouts to test connectivity to specified host and port
    # if was able to connect, should see "Idle timeout expired" and non-zero exit code
    # if was not able to connect, should see "Connection timed out" and non-zero exit code
    # will get NetworkError if cannot ssh to source host
    # --send-only is to ensure that the data sent by the destination (e.g., from mysql) does no get mixed with the ncat
    #   messages; we could also use -o <file> to save it if needed

    # run via subprocess or fabric's run command; both ways we set stdouterr_text and return_code variables to
    # interpret the result
    stdouterr_text = None
    return_code = None
    check_command = "ncat %s %d -i 5ms -w %ds --send-only" % (ip_addr,port,access_check_connection_timeout)
    if env.host_string == "127.0.0.1": # run locally rather than on a remote host -- may not be able to ssh in locally
        try:
            stdouterr_text = subprocess.check_output(check_command, shell=True, stderr=subprocess.STDOUT)
            return_code = 0
        except subprocess.CalledProcessError, e:
            return_code = e.returncode
            stdouterr_text = e.output
    else: # run on remote host
        with hide('warnings'):
            with show('running'):
                with time_limit(overall_check_timeout): # if running this takes longer than the specified amout of
                                                        # time, a TimeoutException will be raised
                    output = run(check_command,timeout=ncat_timeout)
        stdouterr_text = str(output)
        return_code = output.return_code

    #print ">>"+stdouterr_text+"<<"
    if return_code == 0:
        # there was a zero exit code from nc (maybe connection succeeded but was very quickly terminated by server)
        raise RuntimeError("got zero exit code from running '{}': {}".format(check_command,stdouterr_text))

    # gather info about what appears to have happened
    connect_failed_timeout = first_str_match_in_stdout_lines(stdouterr_text,"Ncat: Connection timed out.") \
                             or first_str_match_in_stdout_lines(stdouterr_text,"Ncat: Operation timed out.")
    connect_failed_refused = first_str_match_in_stdout_lines(stdouterr_text,"Ncat: Connection refused.")
    connect_succeeded = first_str_match_in_stdout_lines(stdouterr_text,"Ncat: Idle timeout expired (5 ms).")

    if (connect_succeeded and not connect_failed_timeout and not connect_failed_refused):
        return [True,None]
    elif (connect_failed_timeout and not connect_succeeded and not connect_failed_refused):
        return [False,'connection timeout']
    elif (connect_failed_refused and not connect_succeeded and not connect_failed_timeout):
        return [False,'connection refused']
    else:
        raise RuntimeError("Unclear result from netcat to %s tcp/%d:\n%s" % (ip_addr,port,stdouterr_text))

# do an access test using fabric (fabric already knows the source machine to run it on)
def dests_access_test_via_fabric(dests):
    results = []
    for (dest_host, port) in dests:
        test_time = time.time()
        try:
            (connect_succ,additional) = verify_via_fabric_source_can_connect_to_port(dest_host, port)
            result = 'success' if connect_succ else 'failure'
        except (RuntimeError,NetworkError,socket_error,TimeoutException,CommandTimeout) as e:
            # something went wrong in executing test so figure out a message and consider it an error case
            message = type(e).__name__ + ": " + str(e)
            message.replace("\n","\\n")
            print "got exception: %s" % (message,)
            result = 'error'
            additional = message

        print (env.host_string,dest_host,port,result,additional,test_time),"\n"
        results.append([dest_host,port,result,additional,test_time])
    return results

def dests_access_test_for_sources(sources, dests):
    # check the given destinations from all the given sources and list of test results, where the test results is a
    # list of:
    #   subprocess_id (always -1)
    #   source_ip
    #   dest_ip
    #   dest_port
    #   result
    #   result_additional_info
    #   access_test_ts
    print "starting parallel testing of all sources to all dests"

    if len(sources) == 0: # skip testing if there are no sources specified
        return []

    # use fabric to execute dests_access_test_via_fabric with dests as args on each of hosts in sources
    with hide('running'): # supress "Executing task 'dests_access_test_via_fabric'" (printed before is really executing)
        start_time = time.time()
        result_dict = execute(dests_access_test_via_fabric, dests, hosts=sources)
        # our results are in result_dict -- a dict keyed on source IP
        end_time = time.time()
    print "completed testing all sources to all dests; [{},{}] => {} secs\n"\
            .format(start_time, end_time, end_time - start_time)

    # post-process results into results list to return
    results = []
    for source_ip in result_dict:
        test_results = result_dict[source_ip]
        if test_results is not None:
            for test_result in test_results:
                if len(test_result) != 5:
                    warn("wrong number of items in a test result for {}: {}".format(source_ip,str(test_result)))
                    sys.exit(1)
                (dest_ip, dest_port, result, result_additional_info, access_test_ts) = test_result
                results.append([-1, source_ip, dest_ip, dest_port, result, result_additional_info, access_test_ts])
                # possible TODO: check for dests we didn't get a result for and put out error records for those
        else:
            errmsg = "got None as output from fabric for {}, meaning it wasn't able to run checks".format(source_ip)
            for (dest_ip, dest_port) in dests: # give that message for all destinations we are supposed to have
                results.append([-1, source_ip, dest_ip, dest_port, "error", errmsg, None])
    return results


usage_desc = \
    'verify-port-accessibilty.py checks that a each set of sources can connect via TCP to each of a set of ' \
    'destination IP/ports.  It also keep track of sources it has already fully checked in a file and excludes those ' \
    'from testing.  The testing result is appended to a TSV file with these columns: subprocess ID (always -1), ' \
    'source IP, dest IP, dest port, result (success, failure, or error), additional info, test timestamp.'
parser = ArgumentParser(description=usage_desc)
parser.add_argument('sources_file', metavar='sources-file', type=str, help='sources file (one source IP per line)')
parser.add_argument('destinations_file', metavar='destinations-file', type=str,
                    help='destinations file (each line is <dest-IP>,<dest-port>)')
parser.add_argument('already_tested_sources_file', metavar='already-tested-sources-file', type=str,
                    help='file with list of already tested sources (one source IP per line)')
parser.add_argument('results_file', metavar='results-file', type=str, help='the file to write the results to')
args = parser.parse_args()


env.warn_only = True # make errors form running remote commands non-fatal
env.use_shell = False # make it the default that remote commands we send are not run through a shell
env.timeout = 3 # how long we will wait to connect to the source IP; TODO: make it parameterizable
env.parallel = True # execute checks in parallel
env.pool_size = 5 # how many checks to execute in parallel (each is a subprocess); TODO: make it parameterizable

# figure out how long to timeout diffferent things
access_check_connection_timeout = 2 # how many seconds to wait at most for a connection to be set up; TODO: make it
                                    # parameterizable
# to avoid long hangs due to server-side issues with getting a shell, we'll time out our shell commands and our Python
# code that is running checks
# timeout for the command we run in the shell: we'll give an extra 4 seconds for ncat to start up/shut down; that's
# added to how long at most ncat should try to connect for
# TODO: number of seconds should be parameterizable from command line
ncat_timeout = 4 + access_check_connection_timeout
# timeout for whole check attempt: our timeout for the ssh connection + wait for shell prompt (7s) + shell command
# timeout; TODO: wait for shell prompt secs should be parameterizable from command line
overall_check_timeout = env.timeout + 7 + ncat_timeout

# load destinations
destinations = []
with open(args.destinations_file, 'rb') as destsin:
    destsin = csv.reader(destsin) # read the destinations file as CSV
    for (dest_host,dest_port) in destsin:
        destinations.append([dest_host,int(dest_port)])
print "will try sources with these destinations: "+str(destinations)

# load already-tested-sources
already_tested_sources = set()
try:
    with open(args.already_tested_sources_file, 'r') as alreadyin:
        for already_line in alreadyin:
            already_tested_sources.add(already_line.rstrip())
    # print "already tested sources: "+str(already_tested_sources)
except IOError as e:
    pass # it's fine if there is no already-tested-sources content provided

# load in source machines to test access from and exclude previously tested ones
sources_to_use = []
with open(args.sources_file, 'r') as sourcesin:
    for source_line in sourcesin:
        source = source_line.rstrip().lstrip() # trim any whitespace (including the newline)
        if source in already_tested_sources:
            print "skipping {} since it was already tested".format(source)
        else:
            sources_to_use.append(source)

# do the tests
results = dests_access_test_for_sources(sources_to_use, destinations)

print "saving results to {}".format(args.results_file)
with open(args.results_file, 'ab') as result_out_fh:
    resultout = csv.writer(result_out_fh, delimiter='\t') # write to the results file as TSV
    for result in results:
        (subprocess_id, source_ip, dest_ip, dest_port, result, result_additional_info, access_test_ts) = result

        # write out result to results file
        # columns: subprocess_id int, source_ip string, dest_ip string, dest_port int, result string,
        #          result_additional_info string, access_test_ts timestamp
        resultout.writerows([[-1, source_ip, dest_ip, str(dest_port), result, result_additional_info, access_test_ts]])

# if we got this far without exiting abnormally, we should have tested all sources for all destinations, so add them
# to the already tested sources file
with open(args.already_tested_sources_file, 'a') as alreadyout:
    for source in sources_to_use:
        alreadyout.writelines([source+"\n"])


