module Main where

import Prelude hiding (lookup)

import Control.Concurrent
import Control.DeepSeq
import Control.Monad

import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.IORef

import System.Mem

import Data.Disk.Swapper
import Data.Disk.Swapper.Cache
import Data.Disk.Swapper.Snapshot


instance NFData BS.ByteString where
        rnf x = x `seq` ()



main :: IO ()
main = do
        x <- newIORef =<< mkSwapper "data" []
        setCacheRef x =<< mkClockCache 5

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


        wait1 <- newEmptyMVar
        wait2 <- newEmptyMVar

        forkIO $ do
            BSL.writeFile "snapshot1" . runPut =<< putToSnapshot =<< readIORef x
            putMVar wait1 ()

        forkIO $ do
            BSL.writeFile "snapshot2" . runPut =<< putToSnapshot =<< readIORef x
            putMVar wait2 ()

        takeMVar wait1
        takeMVar wait2


        print . BS.length . getting last =<< readIORef x
