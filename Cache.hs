{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ExistentialQuantification #-}

module Cache where

import Control.Concurrent.MVar
import Control.Monad
import Data.IORef

class Cache a v | a -> v where
        addValue :: a -> v -> IO (IO ())


data SomeCache v = forall c. (Cache c v) => SomeCache !c

instance Cache (SomeCache v) v where
        addValue (SomeCache c) = addValue c



data ClockCache a = ClockCache (MVar [(IORef a, IORef Bool)])

mkClockCache :: Int -> IO (ClockCache a)
mkClockCache = return . ClockCache <=< newMVar . cycle <=< 
        mapM (const $ liftM2 (,) (newIORef undefined) (newIORef False)) . enumFromTo 1


instance Cache (ClockCache a) a where
        addValue cache@(ClockCache mdata) x = do
                add =<< takeMVar mdata
                where add ((ix, ikeep):rest) = do
                              keep <- readIORef ikeep
                              modifyIORef ikeep not

                              if keep then add rest
                                      else do writeIORef ix x
                                              putMVar mdata rest
                                              return (writeIORef ikeep True)
