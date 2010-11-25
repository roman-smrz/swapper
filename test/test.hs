module Main where

import Prelude hiding (lookup)

import Control.DeepSeq
import Control.Monad

import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.IORef

import System.Mem

import Data.Disk.Cache
import Data.Disk.Snapshot
import Data.Disk.Swapper


instance NFData BS.ByteString where
        rnf x = x `seq` ()



main :: IO ()
main = do
        cache <- mkClockCache 5
        x <- newIORef =<< mkSwapper "data" cache []

        forM_ [1..7] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'
                performGC

        forM_ [3..4] $ \i -> do
                BS.writeFile "out" . getting (!!i) =<< readIORef x
                performGC

        forM_ [8..10] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'
                performGC

        forM_ [2..6] $ \i -> do
                x' <- return . getting (!!i) =<< readIORef x
                BS.writeFile "out" x'
                performGC

        forM_ [2..6] $ \_ -> do
                modifyIORef x $ changing tail
                performGC

        forM_ [8..10] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'
                performGC

        BSL.writeFile "snapshot0" . runPut =<< putToSnapshot =<< readIORef x

        forM_ [1..7] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'
                performGC

        BSL.writeFile "snapshot1" . runPut =<< putToSnapshot =<< readIORef x
        BSL.writeFile "snapshot2" . runPut =<< putToSnapshot =<< readIORef x

        print . BS.length . getting last =<< readIORef x
