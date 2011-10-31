{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE CPP #-}

-- |
-- Maintainer  : Roman Smrž <roman.smrz@seznam.cz>
-- Stability   : experimental
--
-- This module provides the actual wrapper around functors, which allows them
-- to swap their values to disk. Before any use, original structure have to by
-- turned into appropriate 'Swapper' using function 'mkSwapper'; this has to be
-- done in IO monad. The created object can then be used in normal pure way,
-- when internal side effects concern only database files determined by the
-- first parameter passed to 'mkSwapper'; those should not be altered
-- externally during the program run.
--
-- Because the Swapper is different type than the original functor, lifting
-- functions are provided to allow manipulation with it. Those are 'adding' for
-- functions, which add new elements like ':' or 'Data.Map.insert', 'getting'
-- for function retrieving elements like 'head' or 'Data.Map.lookup',
-- 'changing' for functions changing the structure itself like 'tail'. Concrete
-- examples are provided it the description of the aforementioned functions.
--
-- When creating snapshot using 'putToSnapshot', all items present it structure
-- are saved into the current database file (its name is then recorded to the
-- snapshot itself in order to be able to load in future) and new one is
-- created for items added thereafter. When the snapshot is loaded using
-- 'getFromSnapshot', only indices and auxiliary data are loaded into the
-- memory at that time; values are read on demand from their database file.



-- IMPLEMENTATION
--
-- Swapper is implemented using the functor given as its first type argument,
-- containing a certain "placeholders" (encapsulated in internal Swappable
-- type) instead of (or possibly along with) actual data. When some value is
-- added to the Swapper object, it is assigned unique numeric key (each Swapper
-- contains counter for generating these keys) to be used later for indexing in
-- database and the Swappable object is created for it. It contains the key and
-- weak pointer to the data; when those are garbage collected and about to be
-- freed, they are written to the database file of the containing Swapper.
--
-- Because loading and deserializing values form database even if such value is
-- frequently used, is undesirable, a cache mechanism is provided using the
-- ClockCache data type. (Originally, it was meant to accept any type belonging
-- to the Cache class, but as we need to recreate some cache when loading
-- snapshot, we need to know its type, hence type parameter in the Swapper
-- would need to carry that information and Swappers differing only in cache
-- type would not constitute the same type; anyway, this may be changed in the
-- future.) Cache only keeps references to certain set of values (and is
-- informed when some value is requested), so keep even weak pointers in
-- appropriate Swappables alive; when the value drop out of cache (and no
-- longer referenced from any other place in the program), it is finalized (id
-- est, saved to the database) and freed.
--
-- When all the references to some Swappable (the per-value structure) are lost
-- and it is about to be garbage collected, it also (in its finalizer) deletes
-- accompanying entry in the current database.
--
--
-- Snapshots
--
-- Because creating snapshot involves writing to external file (and creating
-- new one), it is not (directly) implement the Serialize class, but Snapshot
-- instead, which allows running IO actions. When snapshot is about to be
-- created, all present values are saved to the current database (even those
-- not currently in memory, because they may be saved in some older database;
-- for this, the Traversable instance is needed), then we wait for all possibly
-- running writes to terminate and close then current DB (and reopen it for
-- further reading), and create new one, with filename determined by given
-- prefix and current counter.
--
-- Loading snapshot is rather straightforward: Accounting data and database
-- filename are extracted from snapshot, then that database is opened for
-- reading, actual data are then retrieved, when they are needed. New database
-- is created, with filename again given by prefix and counter, which is also
-- loaded from snapshot. Due to this, snapshot of Swapper should be created in
-- single-threaded manner. Databases assigned to some snapshot always contain
-- all values of the Swapper on which the putToSnapshot was call; it may
-- contain also some other values, if there were other Swappers "descended"
-- from the same mkSwapper call.


module Data.Disk.Swapper (
        Swapper,
        mkSwapper,
        setCache, setCacheRef,
        adding,
        getting,
        changing,
        applying,
        swapperDBPrefix,
) where


import Control.Concurrent.MVar
import Control.Concurrent.QSemN
import Control.DeepSeq
import Control.Monad hiding (forM)
import Control.Parallel

import Data.ByteString.Lazy as BS hiding (map)
import Data.IORef
import Data.Traversable
import Data.Typeable

import Happstack.Data.Serialize

import System.Mem.Weak
import System.IO.Unsafe

#ifdef TRACE_SAVING
import qualified System.IO as IO
#endif

import Data.Disk.Swapper.Cache
import Data.Disk.Swapper.Snapshot
import qualified Data.Disk.Swapper.TokyoCabinet as TC


type Key = ByteString


-- Datatype containing information for single value – its key, weak pointer to
-- actual data and finalization lock (which is emptied when data is created or
-- loaded and filled when they are saved to disk).
data Swappable a = Swappable
        { saKey :: !Key
        , saValue :: !(MVar (Weak (a, IO ())))
        , saFinalized :: !(MVar Integer)
        }

-- The Swapper type; contains database information, counter for generating keys
-- of newly added items, cache and actual content (consisting of Swappables).
data Swapper f a = Swapper
        { spDB :: !SwapDB
        , spCounter :: !(MVar Integer)
        , spCache :: !(IORef (SomeCache a))
        , spContent :: !(f (Swappable a))
        }

-- Data needed to manage access to database
data SwapDB = SwapDB
        { sdDB :: !(MVar [(Integer, TC.Database)])
        -- List of open databases, the first one is for writing, the rest are
        -- read-only databases, created when loading snapshot (the database
        -- accompanying the snapshot), or when creating snapshot (the old
        -- database, which was formerly used for writing).

        , sdPrefix :: !FilePath
        -- These are to components of database filename, the first is prefix,
        -- after which is appended current value of the counter (which is then
        -- incremented) and finely there goes the extension.

        , sdSem :: QSemN
        -- Semaphore to force synchronization when changing databases.
        }



-- Auxiliary functions for accessing the DB

-- They take into account that data can be read from multiple databases and
-- some locking needed for synchronization when snapshots are created

dbOpen :: FilePath -> IO SwapDB
dbOpen prefix = do
        cdb <- TC.open (prefix ++ "0.tcb")
        tdb <- newMVar [(0,cdb)]
        sem <- newQSemN 1000
        return $ SwapDB { sdDB = tdb, sdPrefix = prefix, sdSem = sem }

withDB :: SwapDB -> ([(Integer, TC.Database)] -> IO a) -> IO a
withDB db f = do
        dbs <- readMVar (sdDB db)
        waitQSemN (sdSem db) 1
        res <- f dbs
        signalQSemN (sdSem db) 1
        return res

dbPut :: SwapDB -> ByteString -> ByteString -> IO Integer
dbPut db key value = withDB db $ \((i,cdb):_) -> do
    ok <- TC.put cdb key value
    if ok then return i else error "Error writing into database"

dbGet :: SwapDB -> ByteString -> IO (Maybe ByteString)
dbGet db key = withDB db get
    where get [] = return Nothing
          get ((_,cdb):rest) = do value <- TC.get cdb key
                                  case value of
                                       res@(Just _) -> return res
                                       Nothing      -> get rest

dbOut :: SwapDB -> ByteString -> IO Bool
dbOut db key = withDB db $ \((_,cdb):_) -> TC.out cdb key


-- Instances of Typeable and Snapshot classes

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
                prefix <- safeGet
                spCnt <- safeGet
                cnt <- safeGet
                mkKeys <- getFromSnapshot

                return $ do
                        mcounter <- newMVar spCnt
                        cache <- newIORef nullCache

                        keys <- mkKeys
                        emptyWeak <- mkWeakPtr undefined Nothing
                        finalize emptyWeak
                        data_ <- forM keys $ \key -> do
                                mval <- newMVar emptyWeak
                                mfin <- newMVar cnt
                                return $ Swappable { saKey = key, saValue = mval, saFinalized = mfin }

                        odb <- TC.open $ prefix ++ show cnt ++ ".tcb#mode=r"
                        ndb <- TC.open $ prefix ++ show (cnt+1) ++ ".tcb"
                        dbs <- newMVar [(cnt+1,ndb),(cnt,odb)]
                        sem <- newQSemN 1000

                        return $ Swapper
                                { spDB = SwapDB { sdDB = dbs, sdPrefix = prefix, sdSem = sem  }
                                , spCounter = mcounter
                                , spCache = cache
                                , spContent = data_
                                }


        putToSnapshot sp = do
#ifdef TRACE_SAVING
                IO.putStrLn "Beginning snapshot"
#endif
                spCnt <- readMVar (spCounter sp)
                putData <- putToSnapshot $ fmap saKey $ spContent sp

                -- Open and add new database for writing
                ((cnt, cdb):rest) <- takeMVar (sdDB $ spDB sp)
                waitQSemN (sdSem $ spDB sp) 1000
                ndb <- TC.open $ (sdPrefix $ spDB sp) ++ show (cnt+1) ++ ".tcb"
                signalQSemN (sdSem $ spDB sp) 1000
                putMVar (sdDB $ spDB sp) ((cnt+1,ndb):(cnt,cdb):rest)

                -- Write necessary content into database
                forM (spContent sp) $ \sa -> do
                    wx <- takeMVar $ saValue sa
                    mx <- deRefWeak wx
                    case mx of
                         Just (x, _) -> do
#ifdef TRACE_SAVING
                             IO.putStrLn $ "Saving (cached) " ++ show (fst (deserialize $ saKey sa) :: Integer)
#endif
                             ok <- TC.put cdb (saKey sa) (serialize x)
                             unless ok $ error "Error writing into DB"

                         Nothing -> do
                             finalize wx
                             fcnt <- takeMVar (saFinalized sa)

                             when (fcnt /= cnt) $ do
                                 -- if the data were finalize to some other DB,
                                 -- write them also to the new one
                                 mbx <- dbGet (spDB sp) (saKey sa)
#ifdef TRACE_SAVING
                                 IO.putStrLn $ "Saving (old) " ++ show (fst (deserialize $ saKey sa) :: Integer)
#endif
                                 case mbx of
                                      Just x  -> do ok <- TC.put cdb (saKey sa) x
                                                    unless ok $ error "Error writing into DB"
                                      Nothing -> error "Data not found in DB (snapshot)!"

                             putMVar (saFinalized sa) $ max fcnt cnt

                    putMVar (saValue sa) wx

                -- Reopen the database we used for snapshot for reading only
                dbs <- takeMVar (sdDB $ spDB sp)
                waitQSemN (sdSem $ spDB sp) 1000

                TC.close cdb
                cdb' <- TC.open $ (sdPrefix $ spDB sp) ++ show cnt ++ ".tcb#mode=r"

                signalQSemN (sdSem $ spDB sp) 1000
                putMVar (sdDB $ spDB sp) $ map (\(i,db) -> if i == cnt then (i,cdb') else (i,db)) dbs

                return $ do
                        safePut (sdPrefix $ spDB sp)
                        safePut spCnt
                        safePut cnt
                        putData


return' :: (Monad m) => a -> m a
return' x = x `pseq` return x


swPutNewWeak :: (Serialize a) => Swappable a -> (a, IO ()) -> SwapDB -> IO ()
swPutNewWeak sa (x, refresh) db = do
        value <- mkWeak x (x, refresh) $ Just $ do
#ifdef TRACE_SAVING
                IO.putStrLn $ "Saving " ++ show (fst (deserialize $ saKey sa) :: Integer)
#endif
                putMVar (saFinalized sa) =<<
                    dbPut db (saKey sa) (serialize x)
#ifdef TRACE_SAVING
                IO.putStrLn $ "Saved " ++ show (fst (deserialize $ saKey sa) :: Integer)
#endif

        takeMVar (saFinalized sa)
        putMVar (saValue sa) value


-- Actual loading of data from database
swLoader :: (Serialize a) => Swapper f a -> Key -> IO (a, IO ())
swLoader sp key = do
#ifdef TRACE_SAVING
        IO.putStrLn $ "Loading " ++ show (fst (deserialize key) :: Integer)
#endif

        value <- dbGet (spDB sp) key
        case value of
             Just serialized -> do
                     let x = fst $ deserialize serialized
                     refresh <- addValueRef (spCache sp) x
                     return (x, refresh)
             Nothing -> error "Data not found in DB!"


-- Takes care about creating Swappables from items newly added to the
-- structure; their initialization and assigning of finalizers.
putSwappable :: (Serialize a, NFData a) => Swapper f a -> a -> Swappable a
putSwappable sp x = deepseq x `pseq` unsafePerformIO $ do
        c <- takeMVar (spCounter sp)
        putMVar (spCounter sp) (c+1)
        let key = serialize c

#ifdef TRACE_SAVING
        IO.putStrLn $ "Creating " ++ show c
#endif

        refresh <- addValueRef (spCache sp) x
        mvalue <- newEmptyMVar
        mfin <- newMVar (-1) -- We need this initialization because swPutNewWeak attempts
                             -- to take this MVar before putting a new weak reference.

        let swappable = Swappable key mvalue mfin
        addFinalizer swappable $ do
#ifdef TRACE_SAVING
                IO.putStrLn ("Deleting "++show c)
#endif
                dbOut (spDB sp) key
                return ()

        swPutNewWeak swappable (x,refresh) (spDB sp)
        return' swappable


-- Retrievs data from swapper, possibly loading it from the database using
-- swLoader, and calls refresh function on appropriate object in cache.
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


mkSwapper :: (Serialize a, NFData a, Functor f)
        => FilePath     -- ^ Prefix of database files
        -> f a          -- ^ Initial data
        -> IO (Swapper f a)

-- ^
-- Creates 'Swapper' from given functor object. The first parameter is prefix
-- from which the name of database files are derived (by appending their index
-- number and database extension), those files should not be altered by
-- external files when the program is running or between saving and loading
-- snapshots.
--
-- The 'Swapper' initially uses the 'NullCache', which does not keep any data
-- in memory, apart from those referenced from other places.

mkSwapper filename v = do
        db <- dbOpen filename
        counter <- newMVar (0 :: Integer)
        cache <- newIORef nullCache

        let swapper = Swapper db counter cache $ fmap (putSwappable swapper) v
         in return swapper


-- |
-- Sets cache for given 'Swapper' object; it determines, which items are to be
-- swapped onto disk, when available slots are used up (and also how many of
-- such slots actually exists in the first place); can be shared among several
-- 'Swappable' objects.

setCache :: Cache c a => Swapper f a -> c -> IO ()
setCache swapper = writeIORef (spCache swapper) . SomeCache


-- |
-- Analogous to the 'setCache', but works with 'Swapper' encosed in an 'IORef'.

setCacheRef :: Cache c a => IORef (Swapper f a) -> c -> IO ()
setCacheRef ref cache = flip setCache cache =<< readIORef ref


-- |
-- Lifting function used for adding new elements to the 'Swapper' object. Needs
-- to be applied to functions like ':' or 'Data.Map.insert' for them to act on
-- Swapper instead of the original structure. Requires the function in its
-- first argument to work for functor containing arbitrary type.
--
-- > a :: Swapper [] Int
-- > let a' = adding (:) 6 a
-- >
-- > b :: Swapper (Map String) Int
-- > let b' = adding (insert "new") 42 b

adding :: (Serialize a, NFData a) =>
        (forall b. b -> f b -> f b) ->
        (a -> Swapper f a -> Swapper f a)
adding f x sp = let sx = putSwappable sp x
                 in sx `pseq` sp { spContent = f sx (spContent sp) }


-- |
-- Function used to lift functions getting elements from inner structure,
-- like 'head' or 'Data.Map.lookup', to functions getting elements from 'Swapper'
-- object. Functions in the first argument needs to work on 'f' containing
-- elements of arbitrary type.
--
-- > a :: Swapper [] Int
-- > let x = getting head a
-- >
-- > b :: Swapper (Map String) Int
-- > let y = getting (lookup "some") b

getting :: (Serialize a) => (forall b. f b -> b) -> (Swapper f a -> a)
getting f sp = getSwappable sp $ f $ spContent sp


-- |
--
-- This function is needed to make functions changing the structure somehow
-- (like 'tail' or 'Data.Map.delete'), to change the 'Swapper' instead. Like
-- the previous lifting functions, its first argument needs to work for any
-- values of any type.
--
-- > a :: Swapper [] Int
-- > let a' = changing tail a
-- >
-- > b :: Swapper (Map String) Int
-- > let b' = changing (delete "some") b

changing :: (Serialize a) => (forall b. f b -> f b) -> (Swapper f a -> Swapper f a)
changing f sp = sp { spContent = f (spContent sp) }


-- |
-- For computations that do not touch elements of the 'Swapper', but compute
-- something from the structure of the functor itself, like length or null, is
-- intended the function 'applying'.
--
-- > a :: Swapper [] Int
-- > let l = applying length a
--
-- > b :: Swapper (Map String) Int
-- > let n = applying null b

applying :: (forall b. f b -> a) -> (Swapper f b -> a)
applying f = f . spContent


swapperDBPrefix :: Swapper f a -> FilePath
swapperDBPrefix = sdPrefix . spDB
