all: test test-load

test: *.hs
	ghc --make test.hs -ltokyocabinet

test-load: *.hs
	ghc --make test-load.hs -ltokyocabinet

clean:
	rm -f *.hi *.o test data.tcb
