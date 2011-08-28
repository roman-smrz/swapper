{-# LANGUAGE FlexibleInstances, UndecidableInstances, OverlappingInstances #-}

-- |
-- Maintainer  : Roman Smrž <roman.smrz@seznam.cz>
-- Stability   : experimental
--
-- 'Snapshot' is a type class generalizing 'Serialize', as it, apart from
-- writing values in the 'Put' monad (or reading in 'Get'), allows to perform
-- arbitrary 'IO' actions, like saving data to (or loading from) some external
-- database files. Any instance of 'Serialize' is also trivially an instance of
-- 'Snapshot'.

module Data.Disk.Swapper.Snapshot where

import Control.Monad

import Data.Binary
import Data.Typeable

import Happstack.Data.Serialize


class (Typeable a, Version a) => Snapshot a where
        getFromSnapshot :: Get (IO a)
        putToSnapshot :: a -> IO Put

instance Serialize a => Snapshot a where
        getFromSnapshot = liftM return safeGet
        putToSnapshot = return . (return <=< safePut)
