{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.ByteString.Streaming as SB
import qualified Data.ByteString.Streaming.Char8 as SBC8
import qualified Options.Applicative as OA
import qualified Control.Concurrent.BoundedChan as BC
import qualified Network.Socket.ByteString as NSB
import qualified Network.Socket.ByteString.Lazy as NSBL
import qualified Network.Socket as NS
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Streaming as STRM
import Control.Monad.Trans.Class
import Control.Concurrent
import Control.Monad
import Streaming (Of(..),Stream)
import Data.ByteString (ByteString)
import Data.Word
import System.IO
import Data.Char (ord)
import Foreign

data Settings = Settings
  { settingsBreakpoint :: !Int
  , settingsConnections :: !Int
  , settingsHost :: !String
  , settingsPort :: !String
  , settingsFile :: !FilePath
  }

parser :: OA.Parser Settings
parser = Settings
  <$> OA.option OA.auto (mconcat
    [ OA.long "breakpoint"
    , OA.metavar "BREAKPOINT"
    , OA.short 'b'
    , OA.value 20000
    , OA.help "Reestablish TCP connection after this number of messages"
    ])
  <*> OA.option OA.auto (mconcat
    [ OA.long "connections"
    , OA.metavar "CONNECTIONS"
    , OA.short 'c'
    , OA.value 10
    , OA.help "Number of TCP connections to destination"
    ])
  <*> OA.strArgument (mconcat
    [ OA.metavar "HOST"
    , OA.help "Host to forward logs to"
    ])
  <*> OA.strArgument (mconcat
    [ OA.metavar "PORT"
    , OA.help "Port to forward logs to"
    ])
  <*> OA.strArgument (mconcat
    [ OA.metavar "FILE"
    , OA.help "File from which logs are sourced"
    ])

main :: IO ()
main = do
  s <- OA.execParser (OA.info (OA.helper <*> parser) mempty)
  c <- BC.newBoundedChan (settingsConnections s * 4)
  let killAllThreads = replicateM_ (settingsConnections s) (BC.writeChan c Nothing)
  replicateM_ (settingsConnections s) $ forkIO $ do
    let go = do
          m <- BC.readChan c
          case m of
            Nothing -> return ()
            Just lbs -> do
              addrinfos <- NS.getAddrInfo Nothing (Just (settingsHost s)) (Just (settingsPort s))
              let serveraddr = head addrinfos
              sock <- NS.socket (NS.addrFamily serveraddr) NS.Stream NS.defaultProtocol
              NS.connect sock (NS.addrAddress serveraddr)
              NSBL.sendAll sock lbs
              NS.close sock
    go
  withFile (settingsFile s) ReadMode $ \h -> do
    STRM.mapsM_ (\bsStream -> do
        lbs :> r <- SB.toLazy bsStream
        BC.writeChan c (Just lbs)
        return r
      ) (scanSplitStream 0 (countNewlinesUpTo 10000) (SB.fromHandle h))
    killAllThreads

countNewlinesUpTo :: Int -> Int -> Word8 -> Maybe Int
countNewlinesUpTo !total !current !w8
  | w8 == c2w '\n' = if current < total
      then Just (current + 1)
      else Nothing
  | otherwise = Just current

c2w :: Char -> Word8
c2w = fromIntegral . ord

-- takeLines :: Monad m => Int -> SB.ByteString m r 
--   -> SB.ByteString m (Maybe (SB.ByteString m r))
-- takeLines n = go 0 where
--   go !i s1 = if i < n
--     then do
--       s2 <- SBC8.splitAt 1 =<< SBC8.break (=='\n') s1
--       go (i + 1) s2
--     else return s1
-- 
-- takeLines :: Monad m 
--   => Int 
--   -> SB.ByteString m r 
--   -> Stream (SB.ByteString m) m r
-- takeLines n = go 0 where
--   go !i s1 = if i < n
--     then do
--       s2 <- SBC8.splitAt 1 =<< SBC8.break (=='\n') s1
--       go (i + 1) s2
--     else return s1


scanSplitStream :: forall s m r. Monad m
  => s 
  -> (s -> Word8 -> Maybe s) 
  -> SB.ByteString m r 
  -> Stream (SB.ByteString m) m r 
scanSplitStream st0 f = STRM.wrap . go1 (Left st0) where
  go1 :: Either s ByteString -> SB.ByteString m r -> SB.ByteString m (Stream (SB.ByteString m) m r)
  go1 !ebs !b1 = case ebs of
    Left st1 -> do
      estream <- lift (SB.nextChunk b1)
      case estream of
        Left r -> return (return r)
        Right (bs,b2) -> do
          let eix = findIndexScan st1 f bs
          case eix of
            Left st2 -> do
              SB.fromStrict bs
              go1 (Left st2) b2
            Right ix -> case BS.splitAt ix bs of
              (!bsA,!bsB) -> do
                SB.fromStrict bsA
                go1 (Right bsB) b1
    Right bs1 -> do
      let eix = findIndexScan st0 f bs1
      case eix of
        Left st1 -> do
          SB.fromStrict bs1
          go1 (Left st1) b1
        Right ix -> case BS.splitAt ix bs1 of
          (!bsA,!bsB) -> do
            SB.fromStrict bsA
            go1 (Right bsB) b1

-- | Returns left if you make it all the way to the end.
findIndexScan :: s -> (s -> Word8 -> Maybe s) -> ByteString -> Either s Int
findIndexScan st0 f (BSI.PS x s l) = BSI.accursedUnutterablePerformIO $ withForeignPtr x $ \f -> go (f `plusPtr` s) 0 st0
  where
  go !ptr !n !st
    | n >= l = return (Left st)
    | otherwise = do 
        w <- peek ptr
        case f st w of
          Nothing -> return (Right n)
          Just st' -> go (ptr `plusPtr` 1) (n+1) st'
{-# INLINE findIndexScan #-}


