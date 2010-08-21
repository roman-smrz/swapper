module Main where

import Swapper

import Control.DeepSeq
import Control.Monad

import qualified Data.ByteString as BS
import Data.IORef


instance NFData BS.ByteString where
        rnf x = x `seq` ()



main = do
        x <- newIORef =<< mkSwapper "data" []

        forM_ [1..10] $ \_ -> do
                d <- BS.readFile "in"
                x' <- readIORef x
                writeIORef x $! adding (:) d x'

        forM_ [2..6] $ \i -> do
                BS.writeFile "out" . getting (!!i) =<< readIORef x
