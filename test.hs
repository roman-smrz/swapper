module Main where

import Prelude hiding (lookup)

import Swapper
import SwapMap

import Control.DeepSeq
import Control.Monad

import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Maybe
import Data.IORef

import System.Mem

import Cache
import TokyoCabinet


instance NFData BS.ByteString where
        rnf x = x `seq` ()



main = do
        {-

        cache <- mkClockCache 5
        x <- (newIORef =<< mkSwapMap "smap" cache
                :: IO (IORef (SwapMap Integer BS.ByteString)))

        forM_ [1..10] $ \k -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! insert k d x'
                performGC

        forM_ [3..9] $ \k -> do
                BS.writeFile "out" . fromJust . lookup k =<< readIORef x
                performGC
                -}

--{-
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
                --print $ BS.length x'
                BS.writeFile "out" x'
                performGC

        forM_ [2..6] $ \i -> do
                modifyIORef x $ changing tail
                performGC

        forM_ [8..10] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'
                performGC

        --close . spDB =<< readIORef x
                --}
