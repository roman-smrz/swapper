{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Disk.Swapper where

import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.DeepSeq
import Control.Parallel

import Data.ByteString.Lazy as BS hiding (map)
import Data.IORef
import Data.Traversable
import Data.Typeable

import Happstack.Data.Serialize

import System.Mem.Weak
import System.IO.Unsafe

import qualified System.IO as IO

import Data.Disk.Cache
import Data.Disk.Snapshot
import qualified Data.Disk.TokyoCabinet as TC


type Key = ByteString


data Swappable a = Swappable
        { saKey :: !Key
        , saValue :: !(MVar (Weak (a, IO ())))
        , saFinalized :: !(MVar ())
        }

data Swapper f a = Swapper
        { spDB :: !SwapDB
        , spCounter :: !(MVar Integer)
        , spCache :: !(ClockCache a)
        , spContent :: !(f (Swappable a))
        }

data SwapDB = SwapDB
        { sdDB :: !(MVar [TC.Database])
        , sdPrefix :: !FilePath
        , sdCounter :: !(IORef Integer)
        , sdSem :: QSemN
        }




dbOpen :: FilePath -> IO SwapDB
dbOpen prefix = do
        cdb <- TC.open (prefix ++ "0.tcb")
        tdb <- newMVar [cdb]
        counter <- newIORef 0
        sem <- newQSemN 1000
        return $ SwapDB { sdDB = tdb, sdPrefix = prefix, sdCounter = counter, sdSem = sem }

withDB :: SwapDB -> ([TC.Database] -> IO a) -> IO a
withDB db f = do
        dbs <- readMVar (sdDB db)
        waitQSemN (sdSem db) 1
        res <- f dbs
        signalQSemN (sdSem db) 1
        return res

dbPut :: SwapDB -> ByteString -> ByteString -> IO Bool
dbPut db key value = withDB db $ \(cdb:_) -> TC.put cdb key value

dbGet :: SwapDB -> ByteString -> IO (Maybe ByteString)
dbGet db key = withDB db get
        where get [] = return Nothing
              get (cdb:rest) = do value <- TC.get cdb key
                                  case value of
                                       res@(Just _) -> return res
                                       Nothing      -> get rest

dbOut :: SwapDB -> ByteString -> IO Bool
dbOut db key = withDB db $ \(cdb:_) -> TC.out cdb key



instance (Typeable a) => Typeable (Swappable a) where
        typeOf _ = mkTyConApp (mkTyCon "Data.Disk.Swapper.Swappable")
                        [ typeOf (undefined :: a) ]

instance (Typeable1 f, Typeable a) => Typeable (Swapper f a) where
        typeOf _ = mkTyConApp (mkTyCon "Data.Disk.Swapper.Swapper")
                        [ typeOf1 (undefined :: f a), typeOf (undefined :: a) ]



instance Version a => Version (Swapper f a) where mode = Primitive


instance (
        Typeable a, Version a, Serialize a,
        Typeable1 f, Traversable f, Snapshot (f ByteString)
    ) => Snapshot (Swapper f a) where

        getFromSnapshot = do
                spCnt <- safeGet
                prefix <- safeGet
                cnt <- safeGet
                mkCache <- getFromSnapshot
                mkKeys <- getFromSnapshot

                return $ do
                        mcounter <- newMVar spCnt
                        cache <- mkCache

                        keys <- mkKeys
                        emptyWeak <- mkWeakPtr undefined Nothing
                        finalize emptyWeak
                        data_ <- forM keys $ \key -> do
                                mval <- newMVar emptyWeak
                                mfin <- newMVar ()
                                return $ Swappable { saKey = key, saValue = mval, saFinalized = mfin }

                        dbcounter <- newIORef (cnt+1)
                        odb <- TC.open $ prefix ++ show cnt ++ ".tcb#mode=r"
                        ndb <- TC.open $ prefix ++ show (cnt+1) ++ ".tcb"
                        dbs <- newMVar [ndb,odb]
                        sem <- newQSemN 1000

                        return $ Swapper
                                { spDB = SwapDB { sdDB = dbs, sdPrefix = prefix, sdCounter = dbcounter, sdSem = sem  }
                                , spCounter = mcounter
                                , spCache = cache
                                , spContent = data_
                                }


        putToSnapshot sp = do
                IO.putStrLn "Snapshot (start)"
                spCnt <- readMVar (spCounter sp)
                putCache <- putToSnapshot (spCache sp)
                putData <- putToSnapshot $ fmap saKey $ spContent sp

                forM (spContent sp) $ \sa -> do
                        wx <- takeMVar $ saValue sa
                        mx <- deRefWeak wx
                        case mx of
                             Just (x, _) -> dbPut (spDB sp) (saKey sa) (serialize x)
                             Nothing -> do
                                     readMVar (saFinalized sa)

                                     -- TODO: this copying is necessary only when the data
                                     --       were finalized to some older DB:
                                     mbx <- dbGet (spDB sp) (saKey sa)
                                     case mbx of
                                          Just x  -> dbPut (spDB sp) (saKey sa) x
                                          Nothing -> error "Data not found in DB (snapshot)!"

                        putMVar (saValue sa) wx

                (cdb:rest) <- takeMVar (sdDB $ spDB sp)
                waitQSemN (sdSem $ spDB sp) 1000

                cnt <- readIORef (sdCounter $ spDB sp)
                TC.close cdb
                cdb' <- TC.open $ (sdPrefix $ spDB sp) ++ show cnt ++ ".tcb#mode=r"
                ndb <- TC.open $ (sdPrefix $ spDB sp) ++ show (cnt+1) ++ ".tcb"
                writeIORef (sdCounter $ spDB sp) (cnt+1)

                signalQSemN (sdSem $ spDB sp) 1000
                putMVar (sdDB $ spDB sp) (ndb:cdb':rest)

                return $ do
                        safePut spCnt
                        safePut (sdPrefix $ spDB sp)
                        safePut cnt
                        putCache
                        putData


(=<<!) :: (Monad m) => (a -> m b) -> m a -> m b
f =<<! a = a `pseq` (f =<< a)
return' :: (Monad m) => a -> m a
return' x = x `pseq` return x


swPutNewWeak :: (Serialize a) => Swappable a -> (a, IO ()) -> SwapDB -> IO ()
swPutNewWeak sa (x, refresh) db = do
        value <- mkWeak x (x, refresh) $ Just $ do
                IO.putStrLn $ "Ukladani " ++ show (fst (deserialize $ saKey sa) :: Integer)
                dbPut db (saKey sa) (serialize x)
                putMVar (saFinalized sa) ()
                IO.putStrLn $ "Ulozeno " ++ show (fst (deserialize $ saKey sa) :: Integer)

        takeMVar (saFinalized sa)
        putMVar (saValue sa) value


swLoader :: (Serialize a) => Swapper f a -> Key -> IO (a, IO ())
swLoader sp key = do
        IO.putStrLn $ "Nacitani " ++ show (fst (deserialize key) :: Integer)

        value <- dbGet (spDB sp) key
        case value of
             Just serialized -> do
                     let x = fst $ deserialize serialized
                     refresh <- addValue (spCache sp) x
                     return (x, refresh)
             Nothing -> error "Data not found in DB!"


putSwappable :: (Serialize a, NFData a) => Swapper f a -> a -> Swappable a
putSwappable sp x = deepseq x `pseq` unsafePerformIO $ do
        c <- takeMVar (spCounter sp)
        putMVar (spCounter sp) (c+1)
        let key = serialize c

        IO.putStrLn $ "Vytvareni " ++ show c

        refresh <- addValue (spCache sp) x
        mvalue <- newEmptyMVar
        mfin <- newMVar ()

        let swappable = Swappable key mvalue mfin
        addFinalizer swappable $ do
                IO.putStrLn ("Mazani "++show c)
                dbOut (spDB sp) key
                return ()

        swPutNewWeak swappable (x,refresh) (spDB sp)
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
                     finalize wx
                     readMVar (saFinalized sa)

                     pair@(x, _) <- swLoader sp $ saKey sa
                     swPutNewWeak sa pair (spDB sp)
                     return x



mkSwapper :: (Serialize a, NFData a, Functor f) =>
        FilePath -> ClockCache a -> f a -> IO (Swapper f a)
mkSwapper filename cache v = do
        db <- dbOpen filename
        counter <- newMVar (0 :: Integer)

        let swapper = Swapper db counter cache $ fmap (putSwappable swapper) v
         in return swapper


adding :: (Serialize a, NFData a) =>
        (forall b. b -> f b -> f b) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sp = let sx = putSwappable sp x
                 in sx `pseq` sp { spContent = f sx (spContent sp) }


getting :: (Serialize a) => (forall b. f b -> b) -> (Swapper f a -> a)
getting f sp = getSwappable sp $ f $ spContent sp


changing :: (Serialize a) => (forall b. f b -> f b) -> (Swapper f a -> Swapper f a)
changing f sp = sp { spContent = f (spContent sp) }
