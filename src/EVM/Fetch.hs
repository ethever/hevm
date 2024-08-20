{-# LANGUAGE DataKinds #-}

module EVM.Fetch where

import EVM (initialContract, unknownContract)
import EVM.ABI
import EVM.FeeSchedule (feeSchedule)
import EVM.Format (hexText)
import EVM.SMT
import EVM.Solvers
import EVM.Types
import EVM (emptyContract)

import Optics.Core

import Control.Monad.Trans.Maybe
import Data.Aeson hiding (Error)
import Data.Aeson.Optics
import Data.ByteString qualified as BS
import Data.Text (Text, unpack, pack)
import Data.Maybe (fromMaybe)
import Data.List (foldl')
import Data.Text qualified as T
import Data.Vector qualified as RegularVector
import Network.Wreq
import Network.Wreq.Session (Session)
import Network.Wreq.Session qualified as Session
import Numeric.Natural (Natural)
import System.Process
import Control.Monad.IO.Class
import EVM.Effects
import Control.Concurrent (QSemN)
import Control.Concurrent.STM (STM)
import Control.Concurrent.STM.TMVar (TMVar)
import Network.HTTP.Client (HttpException (..), HttpExceptionContent (..))
import Control.Concurrent.QSemN (waitQSemN)
import Control.Concurrent (signalQSemN)
import Control.Monad.STM (atomically)
import Control.Concurrent.STM.TVar (TVar)
import GHC.IO.Unsafe (unsafePerformIO)
import Control.Concurrent.STM (newTVarIO)
import Control.Concurrent.STM (readTVar)
import Control.Concurrent.STM (modifyTVar')
import Control.Concurrent.STM (readTMVar)
import Control.Concurrent.STM (newEmptyTMVarIO)
import Control.Concurrent.STM (putTMVar)
import Optics.Operators.Unsafe ((^?!))
import Control.Exception as E
import Control.Retry
import Network.HTTP.Types.Status (status429)
import Control.Concurrent (newQSemN)

-- | Abstract representation of an RPC fetch request
data RpcQuery a where
  QueryCode    :: Addr         -> RpcQuery BS.ByteString
  QueryBlock   ::                 RpcQuery Block
  QueryBalance :: Addr         -> RpcQuery W256
  QueryNonce   :: Addr         -> RpcQuery W64
  QuerySlot    :: Addr -> W256 -> RpcQuery W256
  QueryChainId ::                 RpcQuery W256

data BlockNumber = Latest | BlockNumber W256
  deriving (Show, Eq)

deriving instance Show (RpcQuery a)

type RpcInfo = Maybe (BlockNumber, Text)

rpc :: String -> [Value] -> Value
rpc method args = object
  [ "jsonrpc" .= ("2.0" :: String)
  , "id"      .= Number 1
  , "method"  .= method
  , "params"  .= args
  ]

class ToRPC a where
  toRPC :: a -> Value

instance ToRPC Addr where
  toRPC = String . pack . show

instance ToRPC W256 where
  toRPC = String . pack . show

instance ToRPC Bool where
  toRPC = Bool

instance ToRPC BlockNumber where
  toRPC Latest          = String "latest"
  toRPC (EVM.Fetch.BlockNumber n) = String . pack $ show n

readText :: Read a => Text -> a
readText = read . unpack

fetchQuery
  :: Show a
  => BlockNumber
  -> (Value -> IO (Maybe Value))
  -> RpcQuery a
  -> IO (Maybe a)
fetchQuery n f q =
  case q of
    QueryCode addr -> do
        m <- f (rpc "eth_getCode" [toRPC addr, toRPC n])
        pure $ do
          t <- preview _String <$> m
          hexText <$> t
    QueryNonce addr -> do
        m <- f (rpc "eth_getTransactionCount" [toRPC addr, toRPC n])
        pure $ do
          t <- preview _String <$> m
          readText <$> t
    QueryBlock -> do
      m <- f (rpc "eth_getBlockByNumber" [toRPC n, toRPC False])
      pure $ m >>= parseBlock
    QueryBalance addr -> do
        m <- f (rpc "eth_getBalance" [toRPC addr, toRPC n])
        pure $ do
          t <- preview _String <$> m
          readText <$> t
    QuerySlot addr slot -> do
        m <- f (rpc "eth_getStorageAt" [toRPC addr, toRPC slot, toRPC n])
        pure $ do
          t <- preview _String <$> m
          readText <$> t
    QueryChainId -> do
        m <- f (rpc "eth_chainId" [toRPC n])
        pure $ do
          t <- preview _String <$> m
          readText <$> t

parseBlock :: (AsValue s, Show s) => s -> Maybe Block
parseBlock j = do
  coinbase   <- LitAddr . readText <$> j ^? key "miner" % _String
  timestamp  <- Lit . readText <$> j ^? key "timestamp" % _String
  number     <- readText <$> j ^? key "number" % _String
  gasLimit   <- readText <$> j ^? key "gasLimit" % _String
  let
   baseFee = readText <$> j ^? key "baseFeePerGas" % _String
   -- It seems unclear as to whether this field should still be called mixHash or renamed to prevRandao
   -- According to https://github.com/ethereum/EIPs/blob/master/EIPS/eip-4399.md it should be renamed
   -- but alchemy is still returning mixHash
   mixhash = readText <$> j ^? key "mixHash" % _String
   prevRandao = readText <$> j ^? key "prevRandao" % _String
   difficulty = readText <$> j ^? key "difficulty" % _String
   prd = case (prevRandao, mixhash, difficulty) of
     (Just p, _, _) -> p
     (Nothing, Just mh, Just 0x0) -> mh
     (Nothing, Just _, Just d) -> d
     _ -> internalError "block contains both difficulty and prevRandao"
  -- default codesize, default gas limit, default feescedule
  pure $ Block coinbase timestamp number prd gasLimit (fromMaybe 0 baseFee) 0xffffffff feeSchedule


type RCache = TVar [(Text, Value, TMVar (Maybe Value))]

cache :: RCache
{-# NOINLINE cache #-}
cache = unsafePerformIO $ newTVarIO []

-- Helper function to look up a request in the cache using STM
lookupCacheSTM :: Text -> Value -> STM (Maybe (TMVar (Maybe Value)))
lookupCacheSTM url x = do
  cacheMap <- readTVar cache
  pure $ lookup (url, x) [((u, v), tmvar) | (u, v, tmvar) <- cacheMap]

-- Helper function to insert a new request into the cache using STM
insertIntoCacheSTM :: Text -> Value -> TMVar (Maybe Value) -> STM ()
insertIntoCacheSTM url x tmvar = modifyTVar' cache (\c -> (url, x, tmvar) : c)


fetchWithSession' :: QSemN -> Text -> Session -> Value -> IO (Maybe Value)
fetchWithSession' sem url sess x = retrying rp shouldRetry $ \_ -> do
  -- Acquire semaphore before making the request
  bracket (waitQSemN sem 1) (const $ signalQSemN sem 1) $ \_ -> do
    -- First check if the request is in the cache
    maybeCacheTMVar <- atomically $ lookupCacheSTM url x

    case maybeCacheTMVar of
      Just tmvar -> do
        -- traceIO $ "read from cache with url: " ++ unpack url ++ " session: " ++ show sess ++ " value: " ++ show x
        atomically $ readTMVar tmvar
      Nothing -> do
        -- traceIO $ "fetch from rpc with url: " ++ unpack url ++ " session: " ++ show sess ++ " value: " ++ show x
        newTMVar <- newEmptyTMVarIO
        atomically $ insertIntoCacheSTM url x newTMVar
        result <- tryRequest url sess x
        atomically $ handleHttpResultsSTM newTMVar result
  where
    rp = exponentialBackoff 1000 <> limitRetries 100
    -- Encapsulates the logic of making the HTTP request
    tryRequest :: Text -> Session -> Value -> IO (Either HttpException (Response Value))
    tryRequest t s v = E.try $ asValue =<< Session.post s (unpack t) v

    -- Handles the result and fills the TMVar accordingly
    handleHttpResultsSTM :: TMVar (Maybe Value) -> Either HttpException (Response Value) -> STM (Maybe Value)
    handleHttpResultsSTM tm rr = do
      let response = case rr of
            Right r -> r ^? (lensVL responseBody) % key "result"
            Left e -> handler e
      putTMVar tm response
      pure response
      where
        handler :: HttpException -> Maybe Value
        -- 429 exception occurred, return Nothing to trigger retry
        handler (HttpExceptionRequest _ (StatusCodeException resp _))
          | resp ^?! lensVL responseStatus == status429 = Nothing
        -- handler (HttpExceptionRequest _ (InternalException _)) = pure Nothing
        -- handler (HttpExceptionRequest _ (ConnectionTimeout)) = pure Nothing
        -- handler (HttpExceptionRequest _ (NoResponseDataReceived)) = pure Nothing
        handler e = throw e
    shouldRetry _ (Just _) = pure False
    shouldRetry _ Nothing = pure True

fetchWithSession :: Text -> Session -> Value -> IO (Maybe Value)
fetchWithSession url sess x = do
  sem <- newQSemN 10
  fetchWithSession' sem url sess x

fetchContractWithSession
  :: BlockNumber -> Text -> Addr -> Session -> IO (Maybe Contract)
fetchContractWithSession n url addr sess = runMaybeT $ do
  let
    fetch :: Show a => RpcQuery a -> IO (Maybe a)
    fetch = fetchQuery n (fetchWithSession url sess)

  code    <- MaybeT $ fetch (QueryCode addr)
  nonce   <- MaybeT $ fetch (QueryNonce addr)
  balance <- MaybeT $ fetch (QueryBalance addr)

  pure $
    initialContract (RuntimeCode (ConcreteRuntimeCode code))
      & set #nonce    (Just nonce)
      & set #balance  (Lit balance)
      & set #external True

fetchSlotWithSession
  :: BlockNumber -> Text -> Session -> Addr -> W256 -> IO (Maybe W256)
fetchSlotWithSession n url sess addr slot =
  fetchQuery n (fetchWithSession url sess) (QuerySlot addr slot)

fetchBlockWithSession
  :: BlockNumber -> Text -> Session -> IO (Maybe Block)
fetchBlockWithSession n url sess =
  fetchQuery n (fetchWithSession url sess) QueryBlock

fetchBlockFrom :: BlockNumber -> Text -> IO (Maybe Block)
fetchBlockFrom n url = do
  sess <- Session.newAPISession
  fetchBlockWithSession n url sess

fetchContractFrom :: BlockNumber -> Text -> Addr -> IO (Maybe Contract)
fetchContractFrom n url addr = do
  sess <- Session.newAPISession
  fetchContractWithSession n url addr sess

fetchSlotFrom :: BlockNumber -> Text -> Addr -> W256 -> IO (Maybe W256)
fetchSlotFrom n url addr slot = do
  sess <- Session.newAPISession
  fetchSlotWithSession n url sess addr slot

fetchChainIdFrom :: Text -> IO (Maybe W256)
fetchChainIdFrom url = do
  sess <- Session.newAPISession
  fetchQuery Latest (fetchWithSession url sess) QueryChainId

http :: Natural -> Maybe Natural -> BlockNumber -> Text -> Fetcher t m s
http smtjobs smttimeout n url q =
  withSolvers Z3 smtjobs smttimeout $ \s ->
    oracle s (Just (n, url)) q

zero :: Natural -> Maybe Natural -> Fetcher t m s
zero smtjobs smttimeout q =
  withSolvers Z3 smtjobs smttimeout $ \s ->
    oracle s Nothing q

-- smtsolving + (http or zero)
oracle :: SolverGroup -> RpcInfo -> Fetcher t m s
oracle solvers info q = do
  case q of
    PleaseDoFFI vals continue -> case vals of
       cmd : args -> do
          (_, stdout', _) <- liftIO $ readProcessWithExitCode cmd args ""
          pure . continue . encodeAbiValue $
            AbiTuple (RegularVector.fromList [ AbiBytesDynamic . hexText . pack $ stdout'])
       _ -> internalError (show vals)

    PleaseAskSMT branchcondition pathconditions continue -> do
         let pathconds = foldl' PAnd (PBool True) pathconditions
         -- Is is possible to satisfy the condition?
         continue <$> checkBranch solvers (branchcondition ./= (Lit 0)) pathconds

    PleaseFetchContract addr base continue -> do
      contract <- case info of
        Nothing -> let
          c = case base of
            AbstractBase -> unknownContract (LitAddr addr)
            EmptyBase -> emptyContract
          in pure $ Just c
        Just (n, url) -> liftIO $ fetchContractFrom n url addr
      case contract of
        Just x -> pure $ continue x
        Nothing -> internalError $ "oracle error: " ++ show q

    PleaseFetchSlot addr slot continue ->
      case info of
        Nothing -> pure (continue 0)
        Just (n, url) ->
         liftIO $ fetchSlotFrom n url addr slot >>= \case
           Just x  -> pure (continue x)
           Nothing ->
             internalError $ "oracle error: " ++ show q

type Fetcher t m s = App m => Query t s -> m (EVM t s ())

-- | Checks which branches are satisfiable, checking the pathconditions for consistency
-- if the third argument is true.
-- When in debug mode, we do not want to be able to navigate to dead paths,
-- but for normal execution paths with inconsistent pathconditions
-- will be pruned anyway.
checkBranch :: App m => SolverGroup -> Prop -> Prop -> m BranchCondition
checkBranch solvers branchcondition pathconditions = do
  conf <- readConfig
  liftIO $ checkSat solvers (assertProps conf [(branchcondition .&& pathconditions)]) >>= \case
    -- the condition is unsatisfiable
    Unsat -> -- if pathconditions are consistent then the condition must be false
      pure $ Case False
    -- Sat means its possible for condition to hold
    Sat _ -> do -- is its negation also possible?
      checkSat solvers (assertProps conf [(pathconditions .&& (PNeg branchcondition))]) >>= \case
        -- No. The condition must hold
        Unsat -> pure $ Case True
        -- Yes. Both branches possible
        Sat _ -> pure EVM.Types.Unknown
        -- Explore both branches in case of timeout
        EVM.Solvers.Unknown -> pure EVM.Types.Unknown
        Error e -> internalError $ "SMT Solver pureed with an error: " <> T.unpack e
    -- If the query times out, we simply explore both paths
    EVM.Solvers.Unknown -> pure EVM.Types.Unknown
    Error e -> internalError $ "SMT Solver pureed with an error: " <> T.unpack e
