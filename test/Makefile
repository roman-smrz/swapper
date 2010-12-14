all: test test-load test-happstack

GHCOPTS+=-ltokyocabinet -i../src

test: *.hs
	ghc --make test.hs $(GHCOPTS)

test-load: *.hs
	ghc --make test-load.hs $(GHCOPTS)

test-happstack: *.hs
	ghc --make test-happstack.hs $(GHCOPTS)

clean:
	rm -f *.hi *.o test test-load data* snapshot*
