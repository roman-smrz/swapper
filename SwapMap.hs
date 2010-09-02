{-# LANGUAGE ExistentialQuantification #-}

module SwapMap where


import Control.Concurrent.MVar
import Control.DeepSeq
import Control.Monad
import Control.Parallel

import Data.ByteString.Lazy as BS
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe
import Data.IORef

import Happstack.Data.Serialize

import System.Mem
import System.Mem.Weak
import System.IO.Unsafe
import qualified System.IO as IO

import Cache
import qualified TokyoCabinet as TC


data (Ord k, Serialize k, Serialize a) => SwapMap k a = SwapMap (MVar (SwapMapData k a))

data SwapMapData k a = forall c. (Cache c k) => Current
                        { smDB :: TC.Database
                        , smCache :: (MVar c)
                        , smMem :: (MVar (Map k (Weak a, MVar ())))
                        , smCopy :: IO (SwapMapData k a)
                        }
                     | Diff k (Maybe a) (MVar (SwapMapData k a))

isCurrent (Diff _ _ _) = False
isCurrent _ = True


mkSwapMap :: (Ord k, Serialize k, Serialize a, Cache c k) => FilePath -> c -> IO (SwapMap k a)
mkSwapMap dir cache = do
        db <- TC.open $ dir ++ "/0.tcb"
        mcache <- newMVar cache
        mmem <- newMVar M.empty

        counter <- newIORef 1
        let copy odb = do
                c <- atomicModifyIORef counter (\c -> (c+1,c))
                let newname = dir ++ "/" ++ show c ++ ".tcb"

                TC.copy odb newname
                ndb <- TC.open newname
                nmem <- newMVar M.empty

                return $ Current ndb mcache nmem (copy ndb)

        return . SwapMap =<< newMVar (Current db mcache mmem (copy db))


lookup :: (Show k, Ord k, Serialize k, Serialize a) => k -> SwapMap k a -> Maybe a
lookup key (SwapMap mdata) = unsafePerformIO $ do
        smd <- takeMVar mdata
        result <- internalLookup key smd
        putMVar mdata smd
        return result


internalLookup :: (Show k, Ord k, Serialize k, Serialize a) => k -> SwapMapData k a -> IO (Maybe a)

internalLookup key smd@(Current db _ mmem _) = modifyMVar mmem $ \mem ->
        case M.lookup key mem of
             Nothing -> load mem
             Just (weak, lock) -> do
                     mbx <- deRefWeak weak
                     case mbx of
                          Nothing -> readMVar lock >> load mem
                          jx -> return (mem, jx)

        where load mem = do
                     result <- return . fmap (fst.deserialize) =<< TC.get db (serialize key)
                     case result of
                          Just val -> do
                                  IO.putStrLn $ "Loaded "++show key
                                  weak <- smWeak smd key val
                                  return (M.insert key weak mem, result)
                          Nothing -> return (mem, result)


internalLookup key (Diff key' val mnext)
        | key' == key  =  return val
        | otherwise    = withMVar mnext (internalLookup key)


insert :: (Show k, Ord k, Serialize k, Serialize a, NFData a) => k -> a -> SwapMap k a -> SwapMap k a
insert key val (SwapMap mdata) = deepseq val `pseq` unsafePerformIO $ do
        smd <- takeMVar mdata
        case smd of
             Current _ _ mmem _ -> do
                     IO.putStrLn $ "Creating "++show key
                     orig <- internalLookup key smd
                     weak <- smWeak smd key val
                     modifyMVar_ mmem (return . M.insert key weak)

                     ndata <- newMVar smd
                     putMVar mdata $ Diff key orig ndata
                     return $ SwapMap ndata



smWeak :: (Show k, Ord k, Serialize k, Serialize a) => SwapMapData k a -> k -> a -> IO (Weak a, MVar ())
smWeak smd key val = do
        lock <- newEmptyMVar
        liftM (flip (,) lock) $ mkWeakPtr val $ Just $ do
                modifyMVar_ (smMem smd) $ \mem -> do
                        IO.putStrLn $ "Saving "++show key
                        TC.put (smDB smd) (serialize key) (serialize val)
                        IO.putStrLn $ "Saved "++show key
                        putMVar lock ()
                        return $ M.delete key mem
