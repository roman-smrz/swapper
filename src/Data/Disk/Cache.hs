{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveDataTypeable #-}

-- |
-- Maintainer  : Roman Smrž <roman.smrz@seznam.cz>
-- Stability   : experimental
--
-- Here is provided a 'Cache' type class designed to generalize notion of cache
-- currently used in Swapper.


module Data.Disk.Cache where

import Control.Concurrent.MVar
import Control.Monad

import Data.Binary.Get
import Data.Binary.Put
import Data.IORef
import Data.Typeable

import Happstack.Data

import Data.Disk.Snapshot


-- |
-- First parameter of this class is actual cache type, the second is type of
-- cached values. The cache should just hold an reference to those values that
-- are ment to be kept in memory.

class Cache a v | a -> v where
        -- |
        -- This function is called when new value is about to be added to the
        -- structure using the cache. It returns another 'IO' action used for
        -- \"refreshing\"; i.e. which should be called whenever associated value
        -- is accessed, so the cache can adapt.

        addValue :: a -> v -> IO (IO ())


data SomeCache v = forall c. (Cache c v) => SomeCache !c

instance Cache (SomeCache v) v where
        addValue (SomeCache c) = addValue c



-- | Provides standars clock cache with second chance mechanism.
data ClockCache a = ClockCache { ccSize :: Int,  ccData :: MVar [(IORef a, IORef Bool)] }
        deriving (Typeable)

mkClockCache :: Int -> IO (ClockCache a)
mkClockCache size = return . ClockCache size =<< newMVar . cycle =<< 
        mapM (const $ liftM2 (,) (newIORef undefined) (newIORef False)) [1..size]


instance Cache (ClockCache a) a where
        addValue cache x = do
                add =<< takeMVar (ccData cache)
                where add ((ix, ikeep):rest) = do
                              keep <- readIORef ikeep
                              modifyIORef ikeep not

                              if keep then add rest
                                      else do writeIORef ix x
                                              putMVar (ccData cache) rest
                                              return (writeIORef ikeep True)

                      add [] = error "ClockCache: we got to the end of infinite list"


instance Version (ClockCache a)

instance (Typeable a) => Snapshot (ClockCache a) where
        getFromSnapshot = return . mkClockCache . fromIntegral =<< getWord64le
        putToSnapshot = return . (putWord64le.fromIntegral.ccSize)
