{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, FunctionalDependencies #-}

module Cache where

import Control.Monad
import Data.IORef

class Cache a v | a -> v where
        addValue :: a -> v -> IO (IO ())


data ClockCache a = ClockCache (IORef [(IORef a, IORef Bool)])

mkClockCache :: Int -> IO (ClockCache a)
mkClockCache = return . ClockCache <=< newIORef . cycle <=< 
        mapM (const $ liftM2 (,) (newIORef undefined) (newIORef False)) . enumFromTo 1


instance Cache (ClockCache a) a where
        addValue cache@(ClockCache idata) x = do

                ((ix, ikeep):_) <- readIORef idata
                modifyIORef idata tail

                keep <- readIORef ikeep
                modifyIORef ikeep not

                if keep then addValue cache x
                        else writeIORef ix x >> return (writeIORef ikeep True)
