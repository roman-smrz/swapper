test: *.hs
	ghc --make test.hs -ltokyocabinet

clean:
	rm -f *.hi *.o test data.tcb
