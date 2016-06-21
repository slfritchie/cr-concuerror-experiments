REBAR ?= ./rebar
EUNIT_OPTS ?=
CONCUERROR ?= /usr/local/src/Concuerror/concuerror
CONC_OPTS ?=

.PHONY: compile-no-deps test docs xref dialyzer-run dialyzer-quick dialyzer \
		cleanplt upload-docs

compile:
	$(REBAR) compile

compile-no-deps:
	${REBAR} compile skip_deps=true

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) -r clean

test: compile
	${REBAR} ${EUNIT_OPTS} eunit -v skip_deps=true

compile-test:
	$(REBAR) eunit -v skip_deps=true suites=log_server_test tests=foo_test

conc_write1_test: compile-test
	$(CONCUERROR) --pz .eunit -m log_server_test -t $@ $(CONC_OPTS)

conc_write_repair1_test: compile-test
	$(CONCUERROR) --pz .eunit -m log_server_test -t $@ $(CONC_OPTS)

conc_write_repair2_test: compile-test
	$(CONCUERROR) --pz .eunit -m log_server_test -t $@ $(CONC_OPTS)

conc_write_repair3_test: compile-test
	$(CONCUERROR) --pz .eunit -m log_server_test -t $@ $(CONC_OPTS)

