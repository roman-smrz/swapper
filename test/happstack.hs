{-# LANGUAGE TemplateHaskell, DeriveDataTypeable, MultiParamTypeClasses,
        TypeFamilies, FlexibleContexts, FlexibleInstances, TypeOperators #-}

module Main where

import Prelude hiding (lookup)

import Control.DeepSeq
import Control.Monad
import Control.Monad.State

import qualified Data.ByteString as BS
import Data.Typeable

import Happstack.State

import System.IO.Unsafe
import System.Mem

import Data.Disk.Cache
import Data.Disk.Swapper
import Data.Disk.Swapper.HappstackCompat


data AppState = AppState (Swapper [] BS.ByteString)
    deriving Typeable

rootState :: Proxy AppState
rootState = Proxy

instance Version AppState


-- initial swapper to be used when none was loaded from snapshot
{-# NOINLINE initialSwapper #-}
initialSwapper :: Swapper [] BS.ByteString
initialSwapper = unsafePerformIO $ mkSwapper "_local/happstack_state/data" []

instance Component AppState where
    type Dependencies AppState = End
    initialValue = AppState initialSwapper

instance Serialize AppState where
    getCopy = contain $ fmap AppState safeGet
    putCopy (AppState s) = contain $ safePut s


-- instance needed for Swapper
instance NFData BS.ByteString where
    rnf x = x `seq` ()



-- this need to be call at the startup to initialize the cache
initCache :: Update AppState ()
initCache = do
    -- we need to force evaluation of the IO computation, hence the () <-
    () <- gets $ \(AppState s) -> unsafePerformIO $ do
        putStrLn "initCache"
        setCache s =<< mkClockCache 5
    return ()


-- adds entry to the swapper in global state
addState :: BS.ByteString -> Update AppState ()
addState x = do
    AppState s <- get
    let s' = adding (:) x s
    put $ AppState s'

    -- evaluate the Swapper object to HNF in order to allow the data to be
    -- swapped-out (instead of being held by unevaluated thunks)
    s' `seq` return ()


$(mkMethods ''AppState ['addState, 'initCache])



main :: IO ()
main = do
    control <- startSystemState rootState
    update $ InitCache

    forM_ [1..10] $ \_ -> do
        d <- BS.readFile "in"
        update $ AddState d
        performGC

    -- we need to call this wrapper around createCheckpoint
    createSwapperCheckpoint control

    shutdownSystem control
