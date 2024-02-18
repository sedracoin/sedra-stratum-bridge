package sedrastratum

import (
	"context"
	"fmt"
	"time"

	"github.com/sedracoin/sedrad/app/appmessage"
	"github.com/sedracoin/sedrad/infrastructure/network/rpcclient"
	"github.com/sedracoin/Sedra-stratum-bridge/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type SedraApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	sedrad        *rpcclient.RPCClient
	connected     bool
}

func NewSedraAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger) (*SedraApi, error) {
	client, err := rpcclient.NewRPCClient(address)
	if err != nil {
		return nil, err
	}

	return &SedraApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "sedraapi:"+address)),
		sedrad:        client,
		connected:     true,
	}, nil
}

func (sd *SedraApi) Start(ctx context.Context, blockCb func()) {
	sd.waitForSync(true)
	go sd.startBlockTemplateListener(ctx, blockCb)
	go sd.startStatsThread(ctx)
}

func (sd *SedraApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			sd.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			dagResponse, err := sd.sedrad.GetBlockDAGInfo()
			if err != nil {
				sd.logger.Warn("failed to get network hashrate from sedra, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := sd.sedrad.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				sd.logger.Warn("failed to get network hashrate from sedra, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (sd *SedraApi) reconnect() error {
	if sd.sedrad != nil {
		return sd.sedrad.Reconnect()
	}

	client, err := rpcclient.NewRPCClient(sd.address)
	if err != nil {
		return err
	}
	sd.sedrad = client
	return nil
}

func (s *SedraApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking sedrad sync state")
	}
	for {
		clientInfo, err := s.sedrad.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from sedrad @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("Sedra is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("sedrad synced, starting server")
	}
	return nil
}

func (s *SedraApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	blockReadyChan := make(chan bool)
	err := s.sedrad.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
		blockReadyChan <- true
	})
	if err != nil {
		s.logger.Error("fatal: failed to register for block notifications from sedra")
	}

	ticker := time.NewTicker(s.blockWaitTime)
	for {
		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking sedrad sync state, attempting reconnect: ", err)
			if err := s.reconnect(); err != nil {
				s.logger.Error("error reconnecting to sedrad, waiting before retry: ", err)
				time.Sleep(5 * time.Second)
			}
		}
		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (sd *SedraApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	template, err := sd.sedrad.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`'%s' via sedracoin/sedra-stratum-bridge_%s`, client.RemoteApp, version))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from sedra")
	}
	return template, nil
}
