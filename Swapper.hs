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


data Swapeable a = Swapeable !FilePath !(MVar ()) !(IORef (Weak a))
data Swapper f a = Swapper !FilePath !(MVar ()) !(IORef Integer) !(f (Swapeable a))


f =<<! a = a `pseq` (f =<< a)
return' x = x `pseq` return x


swWeak x path = mkWeakPtr x $ Just $ do
        IO.putStrLn $ "Ukladani " ++ path
        BS.writeFile path $ runPut (safePut x)
        IO.putStrLn $ "Ulozeno " ++ path


putSwapeable :: (Serialize a, NFData a) => Swapper f a -> a -> Swapeable a
putSwapeable (Swapper dir lock counter _) x = deepseq x `pseq` unsafePerformIO $ do
        takeMVar lock
        c <- readIORef counter
        modifyIORef counter (+1)
        putMVar lock ()

        let path = dir ++ "/" ++ show c
        IO.putStrLn $ "Vytvareni " ++ path
        nlock <- newMVar ()
        return' . Swapeable path nlock =<<
                newIORef =<<! swWeak x path


getSwapeable :: (Serialize a) => Swapeable a -> a
getSwapeable (Swapeable path lock iwx) = unsafePerformIO $ do
        takeMVar lock
        mx <- deRefWeak =<< readIORef iwx
        x <- case mx of
                  Just x -> return x
                  Nothing -> do
                          IO.putStrLn $ "Nacitani " ++ path
                          x <- return . runGet safeGet =<< BS.readFile path
                          wx <- swWeak x path
                          writeIORef iwx wx
                          return x
        putMVar lock ()
        return x



mkSwapper :: (Serialize a, NFData a, Functor f) => FilePath -> f a -> IO (Swapper f a)
mkSwapper dir v = do
        lock <- newMVar ()
        counter <- newIORef (0 :: Integer)
        let swapper = Swapper dir lock counter $ fmap (putSwapeable swapper) v
         in return swapper


adding :: (Serialize a, NFData a) => ((Swapeable a) -> f (Swapeable a) -> f (Swapeable a)) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sw@(Swapper dir lock counter v) =
        let sx = putSwapeable sw x
         in sx `pseq` Swapper dir lock counter $ f sx v


getting :: (Serialize a) => (f (Swapeable a) -> (Swapeable a)) -> (Swapper f a -> a)
getting f sw@(Swapper _ _ _ xs) = getSwapeable $ f xs
