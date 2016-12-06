# verify-port-accessibility

verify-port-accessibilty.py checks that each of a set of sources can 
connect via TCP to each of a set of destinations (<IP,port>).  It also
keeps track of sources it has already fully checked in a file and
excludes  those from testing.  The testing result is appended to a TSV 
file with these columns:
* subprocess ID (always -1 currently)
* source IP
* dest IP
* dest port
* result
* additional info about result
* time of testing

The result column has "success" if could connect, "failure" if could not
connect, or "error" if there was an error in checking such as source
host not being reachable.

This script has some rough spots but handles a number of corner cases we
have encountered in doing millions of access checks.  For example, it 
handles when you can ssh to a source but you don't get a shell prompt in
any reasonable amount of time.

The script will do up to 5 access checks in parallel, making it much 
faster than our original one-at-a-time approach.