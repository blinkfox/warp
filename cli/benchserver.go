/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/bench"
)

const warpServerVersion = 1

type serverRequestOp string

const (
	serverReqDisconnect  serverRequestOp = "disconnect"
	serverReqBenchmark                   = "benchmark"
	serverReqStartStage                  = "start_stage"
	serverReqStageStatus                 = "stage_status"
	serverReqSendOps                     = "send_ops"
)

const serverFlagName = "serve"

type serverInfo struct {
	ID        string `json:"id"`
	Secret    string `json:"secret"`
	Version   int    `json:"version"`
	connected bool
}

// validate the serverinfo.
func (s serverInfo) validate() error {
	if s.ID == "" {
		return errors.New("no server id sent")
	}
	if s.Version != warpServerVersion {
		return errors.New("warp server and client version mismatch")
	}
	return nil
}

// serverRequest requests an operation from the client and expects a response.
type serverRequest struct {
	Operation serverRequestOp `json:"op"`
	Benchmark struct {
		Command string            `json:"command"`
		Args    cli.Args          `json:"args"`
		Flags   map[string]string `json:"flags"`
	}
	Stage     benchmarkStage `json:"stage"`
	StartTime time.Time      `json:"start_time"`
}

// runServerBenchmark will run a benchmark server if requested.
// Returns a bool whether clients were specified.
func runServerBenchmark(ctx *cli.Context) (bool, error) {
	if ctx.String("warp-client") == "" {
		return false, nil
	}

	conns := newConnections(parseHosts(ctx.String("warp-client")))
	if len(conns.hosts) == 0 {
		return true, errors.New("no hosts")
	}
	conns.info = printInfo
	conns.errLn = printError
	defer conns.closeAll()
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	defer monitor.Done()
	monitor.SetLnLoggers(printInfo, printError)
	var infoLn = monitor.InfoLn
	var errorLn = monitor.Errorln

	var allOps bench.Operations

	// Serialize parameters
	excludeFlags := map[string]struct{}{
		"warp-client":        {},
		"warp-client-server": {},
		"serverprof":         {},
		"autocompletion":     {},
		"help":               {},
		"syncstart":          {},
		"analyze.out":        {},
	}
	req := serverRequest{
		Operation: serverReqBenchmark,
	}
	req.Benchmark.Command = ctx.Command.Name
	req.Benchmark.Args = ctx.Args()
	req.Benchmark.Flags = make(map[string]string)

	for _, flag := range ctx.Command.Flags {
		if _, ok := excludeFlags[flag.GetName()]; ok {
			continue
		}
		if ctx.IsSet(flag.GetName()) {
			var err error
			req.Benchmark.Flags[flag.GetName()], err = flagToJSON(ctx, flag)
			if err != nil {
				return true, err
			}
		}
	}

	// Connect to hosts, send benchmark requests.
	for i := range conns.hosts {
		resp, err := conns.roundTrip(i, req)
		fatalIf(probe.NewError(err), "不能发送基准测试数据给 warp 客户端")
		if resp.Err != "" {
			fatalIf(probe.NewError(errors.New(resp.Err)), "从 warp 客户端接收到了错误信息")
		}
		infoLn("客户端 ", conns.hostName(i), " 已连接 ...")
		// Assume ok.
	}
	infoLn("所有客户端均已连接 ...")

	_ = conns.startStageAll(stagePrepare, time.Now().Add(time.Second), true)
	err := conns.waitForStage(stagePrepare, true)
	if err != nil {
		fatalIf(probe.NewError(err), "准备失败")
	}
	infoLn("所有客户端都已准备 ...")

	const benchmarkWait = 3 * time.Second

	prof, err := startProfiling(context.Background(), ctx)
	if err != nil {
		return true, err
	}
	err = conns.startStageAll(stageBenchmark, time.Now().Add(benchmarkWait), false)
	if err != nil {
		errorLn("无法启动所有客户端", err)
	}
	infoLn("正在所有客户端上运行基准测试 ...")
	err = conns.waitForStage(stageBenchmark, false)
	if err != nil {
		errorLn("无法保持与所有客户端的连接", err)
	}

	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, "remote", time.Now().Format("2006-01-02[150405]"), pRandASCII(4))
	}
	prof.stop(context.Background(), ctx, fileName+".profiles.zip")

	infoLn("已完成. 正在下载相关的请求操作 ...")
	downloaded := conns.downloadOps()
	switch len(downloaded) {
	case 0:
	case 1:
		allOps = downloaded[0]
	default:
		threads := uint16(0)
		for _, ops := range downloaded {
			threads = ops.OffsetThreads(threads)
			allOps = append(allOps, ops...)
		}
	}

	allOps.SortByStartTime()
	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		errorLn("无法写入基准测试数据:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "无法压缩基准测试数据到输出")

			defer enc.Close()
			err = allOps.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "无法写入基准测试数据到输出")

			infoLn(fmt.Sprintf("基准测试数据写入到了 %q\n", fileName+".csv.zst"))
		}()
	}
	monitor.OperationsReady(allOps, fileName, commandLine(ctx))
	printAnalysis(ctx, allOps)

	err = conns.startStageAll(stageCleanup, time.Now(), false)
	if err != nil {
		errorLn("无法清理所有客户端的数据", err)
	}
	err = conns.waitForStage(stageCleanup, false)
	if err != nil {
		errorLn("无法保持与所有客户端的连接", err)
	}
	infoLn("数据清理完成.\n")

	return true, nil
}

// connections keeps track of connections to clients.
type connections struct {
	hosts []string
	ws    []*websocket.Conn
	si    serverInfo
	info  func(data ...interface{})
	errLn func(data ...interface{})
}

// newConnections creates connections (but does not connect) to clients.
func newConnections(hosts []string) *connections {
	var c connections
	c.si = serverInfo{
		ID:      pRandASCII(20),
		Secret:  "",
		Version: warpServerVersion,
	}
	c.hosts = hosts
	c.ws = make([]*websocket.Conn, len(hosts))
	return &c
}

func (c *connections) errorF(format string, data ...interface{}) {
	c.errLn(fmt.Sprintf(format, data...))
}

// closeAll will close all connections.
func (c *connections) closeAll() {
	for i, conn := range c.ws {
		if conn != nil {
			conn.WriteJSON(serverRequest{Operation: serverReqDisconnect})
			conn.Close()
			c.ws[i] = nil
		}
	}
}

// hostName returns the remote host name of a connection.
func (c *connections) hostName(i int) string {
	if c.ws != nil && c.ws[i] != nil {
		return c.ws[i].RemoteAddr().String()
	}
	return c.hosts[i]
}

// hostName returns the remote host name of a connection.
func (c *connections) disconnect(i int) {
	if c.ws[i] != nil {
		c.info("断开客户端连接: ", c.hostName(i))
		c.ws[i].WriteJSON(serverRequest{Operation: serverReqDisconnect})
		c.ws[i].Close()
		c.ws[i] = nil
	}
}

// roundTrip performs a roundtrip.
func (c *connections) roundTrip(i int, req serverRequest) (*clientReply, error) {
	conn := c.ws[i]
	if conn == nil {
		err := c.connect(i)
		if err != nil {
			return nil, err
		}
	}
	for {
		conn := c.ws[i]
		err := conn.WriteJSON(req)
		if err != nil {
			c.errLn(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		var resp clientReply
		err = conn.ReadJSON(&resp)
		if err != nil {
			c.errLn(err)
			if err := c.connect(i); err == nil {
				continue
			}
			return nil, err
		}
		return &resp, nil
	}
}

// connect to a client.
func (c *connections) connect(i int) error {
	tries := 0
	for {
		err := func() error {
			host := c.hosts[i]
			if !strings.Contains(host, ":") {
				host += ":" + strconv.Itoa(warpServerDefaultPort)
			}
			u := url.URL{Scheme: "ws", Host: host, Path: "/ws"}
			c.info("正在连接到 ", u.String())
			var err error
			c.ws[i], _, err = websocket.DefaultDialer.Dial(u.String(), nil)
			if err != nil {
				return err
			}
			sent := time.Now()

			// Send server info
			err = c.ws[i].WriteJSON(c.si)
			if err != nil {
				return err
			}
			var resp clientReply
			err = c.ws[i].ReadJSON(&resp)
			if err != nil {
				return err
			}
			if resp.Err != "" {
				return errors.New(resp.Err)
			}

			roundtrip := time.Since(sent)
			// Add 50% of the roundtrip.
			delta := time.Since(resp.Time.Add(roundtrip / 2))
			if delta < 0 {
				delta = -delta
			}
			if delta > time.Second {
				return fmt.Errorf("host %v time delta too big (%v). Roundtrip took %v. Synchronize clock on client and retry", host, delta.Round(time.Millisecond), roundtrip.Round(time.Millisecond))
			}
			return nil
		}()
		if err == nil {
			return nil
		}
		if tries == 3 {
			c.ws[i] = nil
			return err
		}
		c.errorF("连接失败:%v, 重试中 ...\n", err)
		tries++
		time.Sleep(time.Second)
	}
}

// startStage will start a stage at a specific time on a client.
func (c *connections) startStage(i int, t time.Time, stage benchmarkStage) error {
	req := serverRequest{
		Operation: serverReqStartStage,
		Stage:     stage,
		StartTime: t,
	}
	resp, err := c.roundTrip(i, req)
	if err != nil {
		return err
	}
	if resp.Err != "" {
		c.errorF("客户端 %v 返回了错误信息: %v\n", c.hostName(i), resp.Err)
		return errors.New(resp.Err)
	}
	c.info("客户端 ", c.hostName(i), ": 请求的阶段 ", stage, " 开始了 ...")
	return nil
}

// startStageAll will start a stage at a specific time on all connected clients.
func (c *connections) startStageAll(stage benchmarkStage, startAt time.Time, failOnErr bool) error {
	var wg sync.WaitGroup
	var gerr error
	var mu sync.Mutex
	c.info("请求阶段 ", stage, " 开始 ...")

	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.startStage(i, startAt, stage)
			if err != nil {
				if failOnErr {
					fatalIf(probe.NewError(err), "阶段启动失败.")
				}
				c.errLn("阶段开始失败:", err)
				mu.Lock()
				if gerr == nil {
					gerr = err
				}
				c.ws[i] = nil
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return gerr
}

// downloadOps will download operations from all connected clients.
// If an error is encountered the result will be ignored.
func (c *connections) downloadOps() []bench.Operations {
	var wg sync.WaitGroup
	var mu sync.Mutex
	c.info("正在下载相关请求操作 ...")
	res := make([]bench.Operations, 0, len(c.ws))
	for i, conn := range c.ws {
		if conn == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				resp, err := c.roundTrip(i, serverRequest{Operation: serverReqSendOps})
				if err != nil {
					return
				}
				if resp.Err != "" {
					c.errorF("客户端 %v 返回了错误: %v\n", c.hostName(i), resp.Err)
					return
				}
				c.info("客户端 ", c.hostName(i), ": 相关操作下载完成.")

				mu.Lock()
				res = append(res, resp.Ops)
				mu.Unlock()
				return
			}
		}(i)
	}
	wg.Wait()
	return res
}

// waitForStage will wait for stage completion on all clients.
func (c *connections) waitForStage(stage benchmarkStage, failOnErr bool) error {
	var wg sync.WaitGroup
	for i, conn := range c.ws {
		if conn == nil {
			// log?
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				req := serverRequest{
					Operation: serverReqStageStatus,
					Stage:     stage,
				}
				resp, err := c.roundTrip(i, req)
				if err != nil {
					c.disconnect(i)
					if failOnErr {
						fatalIf(probe.NewError(err), "阶段失败.")
					}
					c.errLn(err)
					return
				}
				if resp.Err != "" {
					c.disconnect(i)
					if failOnErr {
						fatalIf(probe.NewError(errors.New(resp.Err)), "阶段失败. 客户端 %v 返回了错误.", c.hostName(i))
					}
					c.errorF("客户端 %v 返回了错误: %v\n", c.hostName(i), resp.Err)
					return
				}
				if resp.StageInfo.Finished {
					c.info("客户端 ", c.hostName(i), ": 完成了阶段 ", stage, "...")
					return
				}
				time.Sleep(time.Second)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// flagToJSON converts a flag to a representation that can be reversed into the flag.
func flagToJSON(ctx *cli.Context, flag cli.Flag) (string, error) {
	switch flag.(type) {
	case cli.StringFlag:
		if ctx.IsSet(flag.GetName()) {
			return ctx.String(flag.GetName()), nil
		}
	case cli.BoolFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Bool(flag.GetName())), nil
		}
	case cli.Int64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Int64(flag.GetName())), nil
		}
	case cli.IntFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Int(flag.GetName())), nil
		}
	case cli.DurationFlag:
		if ctx.IsSet(flag.GetName()) {
			return ctx.Duration(flag.GetName()).String(), nil
		}
	case cli.UintFlag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Uint(flag.GetName())), nil
		}
	case cli.Uint64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Uint64(flag.GetName())), nil
		}
	case cli.Float64Flag:
		if ctx.IsSet(flag.GetName()) {
			return fmt.Sprint(ctx.Float64(flag.GetName())), nil
		}
	default:
		if ctx.IsSet(flag.GetName()) {
			return "", fmt.Errorf("unhandled flag type: %T", flag)
		}
	}
	return "", nil
}
