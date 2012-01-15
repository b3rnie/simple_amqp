.PHONY:	all check test clean

all:
	./rebar compile

docs:
	./rebar doc

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
