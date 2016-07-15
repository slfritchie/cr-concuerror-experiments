
# Description & Purpose

Experiments using the Concuerror tool to verify that models of Chain
Repair algorithms preserve CR's strong consistency properties.

* [Concuerror](https://github.com/parapluu/Concuerror)
* [The original Chain Replication paper](http://pages.cs.wisc.edu/~dusseau/Classes/CS739/Papers/renesse.pdf)
* [Christopher Meiklejohn's A Brief History of Chain Replication (Papers We Love)](http://paperswelove.org/2015/topic/christopher-meiklejohns-a-brief-history-of-chain-replication/)
* [Formalizing the Chain Replication Protocol (Cornell University)](http://www.nuprl.org/FDLcontentAUXdocs/ChainRepl/)

# Prerequisites

* Erlang/OTP version 17.
    * Older & newer versions may work, but I haven't tested them.
    * We assume that the `erl` command is in your shell's `$PATH` or `$path` or whatever search path.
* Concuerror
    * See the link above
    * Edit line 3 of the Makefile to point to the path of the Concuerror executable

If you encounter the following error when running one of the
Concuerror-specific `make` targets, for example,

    % make
    [...]
    % make conc_write1_test
    [...]
    /usr/local/src/Concuerror/concuerror --pz .eunit -m log_server_test -t conc_write1_test 
    make: /usr/local/src/Concuerror/concuerror: No such file or directory

... then you haven't edited line 3 of the Makefile to Concuerror's
correct path.

# To execute all scenarios in this file:

Here's a short-cut to scrape all of the scenario example command
snippets into a single file and then execute it.  My MacBook Pro
requires about two hours to run all scenarios.

    % egrep '^    %' README.md | sed -e 's/^    % //' | sed 1,2d > /tmp/foo
    % sh -c 'sh -x /tmp/foo 2>&1 | tee /tmp/foo.out'

# Scenario #1

First attempt at a Concuerror model.  Don't perform any repair protocol
yet.  Just have a couple of concurrent writers for a single log
position/key during a concurrent layout change.

Hrm, I managed to screw up something in the Git commit history.  I've
a patch that needs to be applied outside of commit e0871e.

No errors expected.  No errors found by Concuerror, hooray.

    % git checkout 654356d3d36cb137ea598eef17231f5ab45e0500 ; cp fix-repair-ret-val.e0871e.patch /tmp
    % git checkout e0871e71094eeaba1d3e113760fae8ee3f642a4b
    % patch -p1 < /tmp/fix-repair-ret-val.e0871e.patch
    % make clean ; env CONC_OPTS="--depth_bound 250 -o concuerror_report.commit=e0871e.txt.`date +%s`" make conc_write1_test

# Scenario #2

First attempt at a repair protocol, using Machi's scheme.  We know that
there's a race condition which permits a previously written value to be
observed as unwritten.  Let's see if Concuerror can find it.

Yes, Concuerror finds it, hooray.

    % git checkout . ; git checkout 42323df72174416a22cf90e382e1bdb64e0734f3
    % make clean ; env CONC_OPTS="-b 3 --depth_bound 450 -o concuerror_report.commit=42323d.txt.`date +%s`" make conc_write_repair1_test

# Scenario #3

Second attempt at a repair protocol.  This is a new idea, "head of heads"
repair instead of Machi's "tail of tails".  It probably suffers from a
similar problem as Scenario 2, but let's try it anyway.

Concuerror finds a problem.  OK, we won't explore this repair protocol
variation any further.

    % git checkout 3037f8c10a97bbf837d2bf715f6acb6481492aff
    % make clean ; env CONC_OPTS="-b 3 --depth_bound 450 -o concuerror_report.commit=3037f8.txt.`date +%s`" make conc_write_repair2_test

# Scenario #4

We're pretty sure that this new repair protocol is going to work.  It's
divided into two different implementations: starting chain length=1
(difficult/unknown result) and starting chain length>1 (predicted to be
error-free).  There are two different Makefile targets for these two
cases.

Hooray, no problems found in either case!  It works, or I've a bug!  `^_^`

    % git checkout 147a3b3f325070dc9add0e3dfba00d4c7711ecb4
    % make clean ; env CONC_OPTS="-c simple -b 5 --depth_bound 450 -o concuerror_report.commit=14713b.txt.`date +%s`" make conc_write_repair3_1to2_test ; echo done with one ; make clean ; env CONC_OPTS="-c simple -b 5 --depth_bound 450 -o concuerror_report.commit=14713b.txt.`date +%s`" make conc_write_repair3_2to3_test ; echo done with two git checkout master ; echo Reset back to "'master'" branch for safety/sanity
