all: test test-load happstack

GHCOPTS+=-ltokyocabinet -i../src

test: *.hs
	ghc --make test.hs $(GHCOPTS)

test-load: *.hs
	ghc --make test-load.hs $(GHCOPTS)

happstack: *.hs
	ghc --make happstack.hs $(GHCOPTS)

clean:
	rm -f *.hi *.o test test-load data* snapshot*
