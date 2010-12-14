{-# LANGUAGE TemplateHaskell, DeriveDataTypeable, MultiParamTypeClasses,
        TypeFamilies, FlexibleContexts, FlexibleInstances, TypeOperators #-}

module Main where

import Prelude hiding (lookup)

import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Monad
import Control.Monad.State

import qualified Data.ByteString as BS
import Data.Typeable

import Happstack.State

import System.IO.Unsafe

import Data.Disk.Cache
import Data.Disk.Swapper
import Data.Disk.Swapper.HappstackCompat


instance NFData BS.ByteString where
        rnf x = x `seq` ()



data AppState = AppState (Swapper [] BS.ByteString)
        deriving Typeable

rootState :: Proxy AppState
rootState = Proxy

instance Version AppState


{-# NOINLINE initialSwapper #-}
initialSwapper :: MVar (Maybe (Swapper [] BS.ByteString))
initialSwapper = unsafePerformIO $ newMVar Nothing

addState :: BS.ByteString -> Update AppState ()
addState x = modify $ \(AppState s) -> AppState $ adding (:) x s

$(mkMethods ''AppState ['addState])


instance Component AppState where
        type Dependencies AppState = End
        initialValue = unsafePerformIO $ do
                mbsw <- takeMVar initialSwapper
                case mbsw of Just sw -> do
                                     putMVar initialSwapper mbsw
                                     return $ AppState sw
                             Nothing -> do
                                     cache <- mkClockCache 5
                                     sw <- mkSwapper "_local/test-happstack_state/data" cache []
                                     putMVar initialSwapper (Just sw)
                                     return $ AppState sw


instance Serialize AppState where
        getCopy = contain $ fmap AppState safeGet
        putCopy (AppState s) = contain $ safePut s



main :: IO ()
main = do
        control <- startSystemState rootState

        forM_ [1..5] $ \_ -> do
                d <- BS.readFile "in"
                update $ AddState d

        createSwapperCheckpoint control
        shutdownSystem control
