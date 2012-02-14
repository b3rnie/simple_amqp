.PHONY:	all check test clean

all:
	./rebar get-deps
	./rebar compile

docs:
	@:

check:
	./rebar check-plt
	./rebar dialyze

test:
	./rebar eunit

conf_clean:
	@:

clean:
	./rebar clean
	$(RM) doc/*
