{-# LANGUAGE FlexibleContexts, UndecidableInstances, ExistentialQuantification #-}
{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Data.Disk.Swapper.HappstackCompat (
        createSwapperCheckpoint,
) where


import Control.Concurrent.MVar

import Data.Binary
import Data.Binary.Get

import Happstack.Data
import Happstack.State

import System.IO.Unsafe

import Unsafe.Coerce


import Data.Disk.Snapshot
import Data.Disk.Swapper


data X = forall a. X a

{-# NOINLINE checkGet #-}
checkGet :: MVar [(FilePath, X)]
checkGet = unsafePerformIO $ newMVar []

{-# NOINLINE checkPut #-}
checkPut :: MVar [(FilePath, Put)]
checkPut = unsafePerformIO newEmptyMVar


instance Snapshot (Swapper f a) => Serialize (Swapper f a) where

        getCopy = contain . check $ getFromSnapshot
                where check getIO = do
                          prefix <- lookAhead safeGet
                          load <- getIO
                          return . unsafePerformIO $ do
                                  c <- takeMVar checkGet
                                  case lookup prefix c of
                                       Just (X x) -> putMVar checkGet c >> return (unsafeCoerce x)
                                       Nothing -> do x <- load
                                                     putMVar checkGet ((prefix, X x) : c)
                                                     return x

                        

        putCopy sw = contain . check . putToSnapshot $ sw
                where check ioPut = unsafePerformIO $ do
                          c <- takeMVar checkPut
                          let prefix = swapperDBPrefix sw
                          case lookup prefix c of
                               Just p  -> putMVar checkPut c >> return p
                               Nothing -> do p <- ioPut
                                             putMVar checkPut ((prefix, p) : c)
                                             return p



createSwapperCheckpoint :: MVar TxControl -> IO ()
createSwapperCheckpoint txc = do
#ifdef TRACE_SAVING
        putStrLn "createSwapperCheckpoint"
#endif
        putMVar checkPut []
        createCheckpoint txc
        takeMVar checkPut
        return ()
