{-# LANGUAGE ForeignFunctionInterface, EmptyDataDecls #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Data.Disk.Swapper.TokyoCabinet (
        Database,
        open, close,
        put, get, out,
        copy,
) where

import Control.Monad

import Data.IORef

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Unsafe as BSU

import Data.Typeable

import Foreign.C
import Foreign.Ptr
import Foreign.Marshal
import Foreign.Storable

data TCADB deriving Typeable
type Database = Ptr TCADB


foreign import ccall "tcadb.h tcadbnew" tcadbnew :: IO Database
foreign import ccall "tcadb.h tcadbdel" tcadbdel :: Database -> IO ()
foreign import ccall "tcadb.h tcadbopen" tcadbopen :: Database -> CString -> IO Bool
foreign import ccall "tcadb.h tcadbclose" tcadbclose :: Database -> IO Bool
foreign import ccall "tcadb.h tcadbput" tcadbput :: Database -> Ptr () -> CInt -> Ptr () -> CInt -> IO Bool
foreign import ccall "tcadb.h tcadbget" tcadbget :: Database -> Ptr () -> CInt -> Ptr CInt -> IO (Ptr ())
foreign import ccall "tcadb.h tcadbout" tcadbout :: Database -> Ptr () -> CInt -> IO Bool
foreign import ccall "tcadb.h tcadbcopy" tcadbcopy :: Database -> CString -> IO Bool


open :: FilePath -> IO Database
open str = withCAString str $ \filename ->
        do db <- tcadbnew
           tcadbopen db filename
           return db

close :: Database -> IO ()
close db = tcadbclose db >> tcadbdel db


useLBSAsCString :: LBS.ByteString -> (CString -> IO a) -> IO a
useLBSAsCString str action = do
        --allocaBytes (fromIntegral $ LBS.length str) $ \ptr -> do
                ptr <- mallocBytes (fromIntegral $ LBS.length str)
                cur <- newIORef ptr
                forM_ (LBS.toChunks str) $ \ch -> do
                        BSU.unsafeUseAsCString ch $ \cstr -> do
                                dest <- readIORef cur
                                copyBytes dest cstr $ BS.length ch
                                modifyIORef cur (`plusPtr` (BS.length ch))
                result <- action ptr
                free ptr
                return result



put :: Database -> LBS.ByteString -> LBS.ByteString -> IO Bool
put db key value = useLBSAsCString key $ \ckey -> useLBSAsCString value $ \cvalue ->
        tcadbput db (castPtr ckey) (fromIntegral $ LBS.length key) (castPtr cvalue) (fromIntegral $ LBS.length value)


get :: Database -> LBS.ByteString -> IO (Maybe LBS.ByteString)
get db key = useLBSAsCString key $ \ckey -> alloca $ \psize -> do
        value <- tcadbget db (castPtr ckey) (fromIntegral $ LBS.length key) psize
        if value == nullPtr
           then return Nothing
           else do size <- peek psize
                   strict <- BSU.unsafePackCStringFinalizer (castPtr value) (fromIntegral size) (free value)
                   return $ Just $ LBS.fromChunks [strict]


out :: Database -> LBS.ByteString -> IO Bool
out db key = useLBSAsCString key $ \ckey -> tcadbout db (castPtr ckey) (fromIntegral $ LBS.length key)


copy :: Database -> FilePath -> IO Bool
copy db filename = withCAString filename $ tcadbcopy db
