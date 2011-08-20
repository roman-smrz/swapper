all: test test-load happstack in

GHCOPTS+=-ltokyocabinet -i../src

test: *.hs
	ghc --make test.hs $(GHCOPTS)

test-load: *.hs
	ghc --make test-load.hs $(GHCOPTS)

happstack: *.hs
	ghc --make happstack.hs $(GHCOPTS)

in:
	dd if=/dev/zero of=in bs=1024 count=1024

clean:
	rm -f *.hi *.o test test-load data* snapshot*
