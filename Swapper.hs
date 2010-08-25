module Swapper where

import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Parallel

import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Lazy as BS
import Data.IORef

import Happstack.Data.Serialize

import System.Mem
import System.Mem.Weak
import System.IO.Unsafe

import qualified System.IO as IO

import qualified TokyoCabinet as TC


data Swappable a = Swappable
        { saKey :: !ByteString
        , saValue :: !(MVar (Weak a))
        , saDB :: !TC.Database
        , saLoader :: !(MVar (IO a))
        }

data Swapper f a = Swapper
        { spDB :: !TC.Database
        , spCounter :: !(MVar Integer)
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
                        return . fst . deserialize $ value


putSwappable :: (Serialize a, NFData a) => Swapper f a -> a -> Swappable a
putSwappable (Swapper db counter _) x = deepseq x `pseq` unsafePerformIO $ do
        c <- takeMVar counter
        putMVar counter (c+1)

        IO.putStrLn $ "Vytvareni " ++ show c
        mload <- newEmptyMVar
        mvalue <- newMVar =<<! swWeak (serialize c) x db mload
        return' $! Swappable (serialize c) mvalue db mload


getSwappable :: (Serialize a) => Swappable a -> a
getSwappable (Swappable key mwx db mloader) = unsafePerformIO $ do
        wx <- takeMVar mwx
        mx <- deRefWeak wx
        case mx of Just x -> putMVar mwx wx >> return x
                   Nothing -> do
                           loader <- takeMVar mloader
                           x <- loader
                           putMVar mwx =<< swWeak key x db mloader
                           return x



mkSwapper :: (Serialize a, NFData a, Functor f) => FilePath -> f a -> IO (Swapper f a)
mkSwapper filename v = do
        db <- TC.open (filename ++ ".tcb")
        counter <- newMVar (0 :: Integer)
        let swapper = Swapper db counter $ fmap (putSwappable swapper) v
         in return swapper


adding :: (Serialize a, NFData a) => ((Swappable a) -> f (Swappable a) -> f (Swappable a)) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sw@(Swapper mdb counter v) =
        let sx = putSwappable sw x
         in sx `pseq` Swapper mdb counter $ f sx v


--getting :: (Serialize a) => (f (Swappable a) -> (Swappable a)) -> (Swapper f a -> a)
getting f sw@(Swapper _ _ xs) = getSwappable $ f xs
