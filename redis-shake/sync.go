// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"sync"

	"redis-shake/pkg/libs/log"

	"redis-shake/redis-shake/common"
	"redis-shake/redis-shake/configure"
	"redis-shake/redis-shake/dbSync"
)

// main struct
type CmdSync struct {
	dbSyncers []*dbSync.DbSyncer
}

// return send buffer length, delay channel length, target db offset
func (cmd *CmdSync) GetDetailedInfo() interface{} {
	ret := make([]map[string]interface{}, len(cmd.dbSyncers))
	for i, syncer := range cmd.dbSyncers {
		if syncer == nil {
			continue
		}
		ret[i] = syncer.GetExtraInfo()
	}
	return ret
}

//sync 命令主流程入口
func (cmd *CmdSync) Main() {
	//同步节点的信息，id:run_id, slotLeftBoundary: 当前node分配的slot左边界，slotRightBoundary则为右边界
	type syncNode struct {
		id                int
		source            string
		sourcePassword    string
		target            []string
		targetPassword    string
		slotLeftBoundary  int
		slotRightBoundary int
	}

	var slotDistribution []utils.SlotOwner
	var err error
	//如果是redis cluster，且支持断点重连，获取node的slot信息
	if conf.Options.SourceType == conf.RedisTypeCluster && conf.Options.ResumeFromBreakPoint {
		//通过 cluster slots获取slots信息
		if slotDistribution, err = utils.GetSlotDistribution(conf.Options.SourceAddressList[0], conf.Options.SourceAuthType,
			conf.Options.SourcePasswordRaw, false); err != nil {
			log.Errorf("get source slot distribution failed: %v", err)
			return
		}
	}

	// source redis number
	total := utils.GetTotalLink()
	syncChan := make(chan syncNode, total)
	cmd.dbSyncers = make([]*dbSync.DbSyncer, total)
	for i, source := range conf.Options.SourceAddressList {
		var target []string
		if conf.Options.TargetType == conf.RedisTypeCluster {
			target = conf.Options.TargetAddressList
		} else {
			// 如果不是redis cluster，则随机pick一个？
			// round-robin pick
			pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
			target = []string{conf.Options.TargetAddressList[pick]}
		}

		// fetch slot boundary
		//通过source信息获取当前node的slot边界
		leftSlotBoundary, rightSlotBoundary := utils.GetSlotBoundary(slotDistribution, source)

		nd := syncNode{
			id:                i,
			source:            source,
			sourcePassword:    conf.Options.SourcePasswordRaw,
			target:            target,
			targetPassword:    conf.Options.TargetPasswordRaw,
			slotLeftBoundary:  leftSlotBoundary,
			slotRightBoundary: rightSlotBoundary,
		}
		//新node传给sync chan
		syncChan <- nd
	}

	var wg sync.WaitGroup
	wg.Add(len(conf.Options.SourceAddressList))
	//对于每一个node开一个协程做同步逻辑，使用psync/sync命令同步的逻辑在函数db.Sync()中
	for i := 0; i < int(conf.Options.SourceRdbParallel); i++ {
		i := i
		go func() {
			for {
				nd, ok := <-syncChan
				if !ok {
					break
				}

				// one sync link corresponding to one DbSyncer
				ds := dbSync.NewDbSyncer(nd.id, nd.source, nd.sourcePassword, nd.target, nd.targetPassword,
					nd.slotLeftBoundary, nd.slotRightBoundary, conf.Options.HttpProfile + i)
				cmd.dbSyncers[nd.id] = ds
				// run in routine
				//主要逻辑入口
				go ds.Sync()

				// wait full sync done
				<-ds.WaitFull

				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(syncChan)

	// never quit because increment syncing is always running
	select {}
}