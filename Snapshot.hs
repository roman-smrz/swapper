{-# LANGUAGE FlexibleInstances, UndecidableInstances, OverlappingInstances #-}

module Snapshot where

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
