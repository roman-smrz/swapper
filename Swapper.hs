{-# LANGUAGE ExistentialQuantification, FlexibleContexts #-}

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


data Swappable a = forall c. (Cache c a) => Swappable
        { saKey :: !ByteString
        , saValue :: !(MVar (Weak a))
        , saDB :: !TC.Database
        , saLoader :: !(MVar (IO a))
        , saCache :: !(MVar c)
        , saRefresh :: !(IORef (IO ()))
        }

data Swapper f a = forall c. (Cache c a) => Swapper
        { spDB :: !TC.Database
        , spCounter :: !(MVar Integer)
        , spCache :: !(MVar c)
        , spContent :: !(f (Swappable a))
        }


f =<<! a = a `pseq` (f =<< a)
return' x = x `pseq` return x


swWeak :: (Serialize a) => ByteString -> a -> TC.Database -> MVar (IO a) -> IO (Weak a)
swWeak key x db mload =
        mkWeakPtr x $ Just $ do
                IO.putStrLn $ "Ukladani " ++ show (fst (deserialize key) :: Integer)
                TC.put db key (serialize x)
                IO.putStrLn $ "Ulozeno " ++ show (fst (deserialize key) :: Integer)

                putMVar mload $ do
                        IO.putStrLn $ "Nacitani " ++ show (fst (deserialize key) :: Integer)
                        value <- TC.get db key
                        return . fst . deserialize $ fromJust $ value


putSwappable :: (Serialize a, NFData a) => Swapper f a -> a -> Swappable a
putSwappable (Swapper db counter mcache _) x = deepseq x `pseq` unsafePerformIO $ do
        c <- takeMVar counter
        putMVar counter (c+1)
        let sc = serialize c

        IO.putStrLn $ "Vytvareni " ++ show c
        mload <- newEmptyMVar
        mvalue <- newMVar =<<! swWeak sc x db mload
        cache <- takeMVar mcache
        irefresh <- newIORef =<< addValue cache x
        putMVar mcache cache

        let swappable = Swappable sc mvalue db mload mcache irefresh
        addFinalizer swappable $ do
                IO.putStrLn ("Mazani "++show c)
                TC.out db sc
                return ()

        return' swappable


getSwappable :: (Serialize a) => Swappable a -> a
getSwappable sa = unsafePerformIO $ do
        wx <- takeMVar $ saValue sa
        mx <- deRefWeak wx
        case mx of
             Just x -> do
                     -- (!) possibly refreshing now-replaced value
                     join $ readIORef (saRefresh sa)
                     putMVar (saValue sa) wx >> return x

             Nothing -> do
                     loader <- takeMVar (saLoader sa)
                     x <- loader
                     putMVar (saValue sa) =<< swWeak (saKey sa) x (saDB sa) (saLoader sa)
                     return x



mkSwapper :: (Serialize a, NFData a, Functor f, Cache c a) => FilePath -> c -> f a -> IO (Swapper f a)
mkSwapper filename cache v = do
        db <- TC.open (filename ++ ".tcb")
        counter <- newMVar (0 :: Integer)
        mcache <- newMVar cache
        let swapper = Swapper db counter mcache $ fmap (putSwappable swapper) v
         in return swapper


adding :: (Serialize a, NFData a) => ((Swappable a) -> f (Swappable a) -> f (Swappable a)) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sw@(Swapper mdb counter cache v) =
        let sx = putSwappable sw x
         in sx `pseq` Swapper mdb counter cache $ f sx v


getting :: (Serialize a) => (f (Swappable a) -> (Swappable a)) -> (Swapper f a -> a)
getting f sw = getSwappable $ f $ spContent sw


changing :: (Serialize a) => (f (Swappable a) -> f (Swappable a)) -> (Swapper f a -> Swapper f a)
changing f (Swapper db counter cache content) = Swapper db counter cache $ f content
