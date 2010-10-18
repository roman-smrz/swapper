{-# LANGUAGE RankNTypes, FlexibleContexts #-}

module Swapper where

import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Monad
import Control.Parallel

import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Lazy as BS
import Data.IORef
import Data.Maybe

import Happstack.Data.Serialize

import System.Mem
import System.Mem.Weak
import System.IO.Unsafe

import qualified System.IO as IO

import Cache
import qualified TokyoCabinet as TC


type Key = ByteString


data Swappable a = Swappable
        { saKey :: !Key
        , saValue :: !(MVar (Weak (a, IO ())))
        }

data Swapper f a = Swapper
        { spDB :: !TC.Database
        , spCounter :: !(MVar Integer)
        , spCache :: !(SomeCache a)
        , spContent :: !(f (Swappable a))
        }


f =<<! a = a `pseq` (f =<< a)
return' x = x `pseq` return x


swPutNewWeak :: (Serialize a) => MVar (Weak (a, IO ())) -> Key -> (a, IO ()) -> TC.Database -> IO ()
swPutNewWeak mvalue key (x, refresh) db = do
        value <- mkWeak x (x, refresh) $ Just $ do
                -- TODO: even with this lock, data may be requested before they are written to DB
                lock <- takeMVar mvalue
                IO.putStrLn $ "Ukladani " ++ show (fst (deserialize key) :: Integer)
                TC.put db key (serialize x)
                IO.putStrLn $ "Ulozeno " ++ show (fst (deserialize key) :: Integer)
                putMVar mvalue lock
        putMVar mvalue value


swLoader :: (Serialize a) => Swapper f a -> Key -> IO (a, IO ())
swLoader sp key = do
        IO.putStrLn $ "Nacitani " ++ show (fst (deserialize key) :: Integer)

        value <- TC.get (spDB sp) key
        case value of
             Just serialized -> do
                     let x = fst $ deserialize serialized
                     refresh <- addValue (spCache sp) x
                     return (x, refresh)
             Nothing -> error "Data not found in DB!"


putSwappable :: (Serialize a, NFData a) => Swapper f a -> a -> Swappable a
putSwappable sp@(Swapper db counter mcache _) x = deepseq x `pseq` unsafePerformIO $ do
        c <- takeMVar counter
        putMVar counter (c+1)
        let key = serialize c

        IO.putStrLn $ "Vytvareni " ++ show c

        refresh <- addValue (spCache sp) x
        mvalue <- newEmptyMVar
        swPutNewWeak mvalue key (x,refresh) db

        let swappable = Swappable key mvalue
        addFinalizer swappable $ do
                IO.putStrLn ("Mazani "++show c)
                TC.out db key
                return ()

        return' swappable


getSwappable :: (Serialize a) => Swapper f a -> Swappable a -> a
getSwappable sp sa = unsafePerformIO $ do
        wx <- takeMVar $ saValue sa
        mx <- deRefWeak wx
        case mx of
             Just (x, refresh) ->
                     -- TODO: possibly refreshing now-replaced value
                     refresh >> putMVar (saValue sa) wx >> return x

             Nothing -> do
                     pair@(x, _) <- swLoader sp $ saKey sa
                     swPutNewWeak (saValue sa) (saKey sa) pair (spDB sp)
                     return x



mkSwapper :: (Serialize a, NFData a, Functor f, Cache c a) => FilePath -> c -> f a -> IO (Swapper f a)
mkSwapper filename cache v = do
        db <- TC.open (filename ++ ".tcb")
        counter <- newMVar (0 :: Integer)

        let swapper = Swapper db counter (SomeCache cache) $ fmap (putSwappable swapper) v
         in return swapper


adding :: (Serialize a, NFData a) =>
        (forall b. b -> f b -> f b) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sw@(Swapper mdb counter cache v) =
        let sx = putSwappable sw x
         in sx `pseq` Swapper mdb counter cache $ f sx v


getting :: (Serialize a) => (forall b. f b -> b) -> (Swapper f a -> a)
getting f sp = getSwappable sp $ f $ spContent sp


changing :: (Serialize a) => (forall b. f b -> f b) -> (Swapper f a -> Swapper f a)
changing f (Swapper db counter cache content) = Swapper db counter cache $ f content
