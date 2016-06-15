REBAR ?= ./rebar
EUNIT_OPTS ?=

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
