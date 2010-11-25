module Main where

import Control.DeepSeq
import Control.Monad

import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as BSL

import System.Mem

import Data.Disk.Swapper
import Data.Disk.Snapshot

instance NFData BS.ByteString where
        rnf x = x `seq` ()

main :: IO ()
main = do
        x <- runGet getFromSnapshot =<< BSL.readFile "snapshot2"
        print . BS.length . getting last $ x

        let y = adding (:) (BS.pack "aaa") $
                adding (:) (BS.pack "bbb") $
                x

        forM_ [2..12] $ \i -> do
                BS.writeFile "out" (getting (!!i) y)
                performGC

        BSL.writeFile "snapshot3" . runPut =<< putToSnapshot y
