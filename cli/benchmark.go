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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/bench"
)

var benchFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "将基准测试+配置文件的数据输出到此文件. 默认会生成唯一的文件名.",
	},
	cli.StringFlag{
		Name:  "serverprof",
		Usage: "在基准测试期间运行 MinIO 服务器配置文件. 值可以是 'cpu', 'mem', 'block', 'mutex' 和 'trace'.",
		Value: "",
	},
	cli.DurationFlag{
		Name:  "duration",
		Usage: "运行基准测试的持续时间. 使用 's' 和 'm' 来指定秒和分钟数，如：'2m34s'. 默认 5 分钟.",
		Value: 5 * time.Minute,
	},
	cli.BoolFlag{
		Name:  "autoterm",
		Usage: "当基准测试运行稳定时就自动终止.",
	},
	cli.DurationFlag{
		Name:  "autoterm.dur",
		Usage: "输出稳定后就自动终止运行的最短持续时间.",
		Value: 10 * time.Second,
	},
	cli.Float64Flag{
		Name:  "autoterm.pct",
		Usage: "最后的 6/25 个时间段内的运行速度，必须在当前速度内才能自动终止.",
		Value: 7.5,
	},
	cli.BoolFlag{
		Name:  "noclear",
		Usage: "在运行基准测试之前或之后，请不要清除存储桶，因为在运行多个客户端时还需要使用.",
	},
	cli.BoolFlag{
		Name:   "keep-data",
		Usage:  "保留基准测试数据. 基准测试结束后请不要清除数据，下次运行基准测试之前数据会自动被清除.",
		Hidden: true,
	},
	cli.StringFlag{
		Name:  "syncstart",
		Usage: "指定基准测试的开始时间. 时间格式为 'hh:mm'，使用 24h 小时格式.",
		Value: "",
	},
	cli.StringFlag{
		Name:   "warp-client",
		Usage:  "连接到 warp 客户端，并在客户端中运行基准测.",
		EnvVar: "",
		Value:  "",
	},
}

// runBench will run the supplied benchmark and save/print the analysis.
func runBench(ctx *cli.Context, b bench.Benchmark) error {
	activeBenchmarkMu.Lock()
	ab := activeBenchmark
	activeBenchmarkMu.Unlock()
	b.GetCommon().Error = printError
	if ab != nil {
		return runClientBenchmark(ctx, b, ab)
	}
	if done, err := runServerBenchmark(ctx); done || err != nil {
		fatalIf(probe.NewError(err), "运行远程基准测试时出错")
		return nil
	}

	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	monitor.SetLnLoggers(printInfo, printError)
	defer monitor.Done()

	monitor.InfoLn("Preparing server.")
	pgDone := make(chan struct{})
	c := b.GetCommon()
	c.Clear = !ctx.Bool("noclear")
	if ctx.Bool("autoterm") {
		// TODO: autoterm cannot be used when in client/server mode
		c.AutoTermDur = ctx.Duration("autoterm.dur")
		c.AutoTermScale = ctx.Float64("autoterm.pct") / 100
	}
	if !globalQuiet && !globalJSON {
		c.PrepareProgress = make(chan float64, 1)
		const pgScale = 10000
		pg := newProgressBar(pgScale, pb.U_NO)
		pg.ShowCounters = false
		pg.ShowElapsedTime = false
		pg.ShowSpeed = false
		pg.ShowTimeLeft = false
		pg.ShowFinalTime = true
		go func() {
			defer close(pgDone)
			defer pg.Finish()
			tick := time.Tick(time.Millisecond * 125)
			pg.Set(-1)
			pg.SetCaption("准备中: ")
			newVal := int64(-1)
			for {
				select {
				case <-tick:
					current := pg.Get()
					if current != newVal {
						pg.Set64(newVal)
						pg.Update()
					}
					monitor.InfoQuietln(fmt.Sprintf("Preparation: %0.0f%% done...", float64(newVal)/float64(100)))
				case pct, ok := <-c.PrepareProgress:
					if !ok {
						pg.Set64(pgScale)
						if newVal > 0 {
							pg.Update()
						}
						return
					}
					newVal = int64(pct * pgScale)
				}
			}
		}()
	} else {
		close(pgDone)
	}

	err := b.Prepare(context.Background())
	fatalIf(probe.NewError(err), "准备服务端时出错")
	if c.PrepareProgress != nil {
		close(c.PrepareProgress)
		<-pgDone
	}

	// Start after waiting a second or until we reached the start time.
	tStart := time.Now().Add(time.Second * 3)
	if st := ctx.String("syncstart"); st != "" {
		startTime := parseLocalTime(st)
		now := time.Now()
		if startTime.Before(now) {
			monitor.Errorln("无法在同步开始前进行准备")
			tStart = time.Now()
		} else {
			tStart = startTime
		}
	}

	benchDur := ctx.Duration("duration")
	ctx2, cancel := context.WithDeadline(context.Background(), tStart.Add(benchDur))
	defer cancel()
	start := make(chan struct{})
	go func() {
		<-time.After(time.Until(tStart))
		monitor.InfoLn("开始运行基准测试 ...")
		close(start)
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(4)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	prof, err := startProfiling(ctx2, ctx)
	fatalIf(probe.NewError(err), "无法启动 profile 配置文件.")
	monitor.InfoLn("开始启动基准测试 ", time.Until(tStart).Round(time.Second), "...")
	pgDone = make(chan struct{})
	if !globalQuiet && !globalJSON {
		pg := newProgressBar(int64(benchDur), pb.U_DURATION)
		go func() {
			defer close(pgDone)
			defer pg.Finish()
			pg.SetCaption("基准测试中:")
			tick := time.Tick(time.Millisecond * 125)
			done := ctx2.Done()
			for {
				select {
				case t := <-tick:
					elapsed := t.Sub(tStart)
					if elapsed < 0 {
						continue
					}
					pg.Set64(int64(elapsed))
					pg.Update()
					monitor.InfoQuietln(fmt.Sprintf("基准运行中: %0.0f%%...", 100*float64(elapsed)/float64(benchDur)))
				case <-done:
					pg.Set64(int64(benchDur))
					pg.Update()
					return
				}
			}
		}()
	} else {
		close(pgDone)
	}
	ops, _ := b.Start(ctx2, start)
	cancel()
	<-pgDone

	// Previous context is canceled, create a new...
	monitor.InfoLn("正在保存基准测试数据...")
	ctx2 = context.Background()
	ops.SortByStartTime()
	ops.SetClientID(cID)
	prof.stop(ctx2, ctx, fileName+".profiles.zip")

	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		monitor.Errorln("无法写入基准测试数据:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "无法压缩基准测试数据到输出")

			defer enc.Close()
			err = ops.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "无法写入基准测试数据到输出")

			monitor.InfoLn(fmt.Sprintf("基准测试数据写入到了 %q\n", fileName+".csv.zst"))
		}()
	}
	monitor.OperationsReady(ops, fileName, commandLine(ctx))
	printAnalysis(ctx, ops)
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		monitor.InfoLn("开始清理数据 ...")
		b.Cleanup(context.Background())
	}
	monitor.InfoLn("基准测试数据已清理完毕.")
	return nil
}

var activeBenchmarkMu sync.Mutex
var activeBenchmark *clientBenchmark

type clientBenchmark struct {
	sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	results bench.Operations
	err     error
	stage   benchmarkStage
	info    map[benchmarkStage]stageInfo
}

type stageInfo struct {
	startRequested bool
	start          chan struct{}
	done           chan struct{}
}

func (c *clientBenchmark) init(ctx context.Context) {
	c.results = nil
	c.err = nil
	c.stage = stageNotStarted
	c.info = make(map[benchmarkStage]stageInfo, len(benchmarkStages))
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, stage := range benchmarkStages {
		c.info[stage] = stageInfo{
			start: make(chan struct{}),
			done:  make(chan struct{}),
		}
	}
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) waitForStage(s benchmarkStage) error {
	c.Lock()
	info, ok := c.info[s]
	ctx := c.ctx
	c.Unlock()
	if !ok {
		return errors.New("waitForStage: unknown stage")
	}
	select {
	case <-info.start:
		c.setStage(s)
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// waitForStage waits for the stage to be ready and updates the stage when it is
func (c *clientBenchmark) stageDone(s benchmarkStage, err error) {
	console.Infoln(s, "done...")
	if err != nil {
		console.Errorln(err.Error())
	}
	c.Lock()
	info := c.info[s]
	if err != nil && c.err == nil {
		c.err = err
	}
	if info.done != nil {
		close(info.done)
	}
	c.Unlock()
}

func (c *clientBenchmark) setStage(s benchmarkStage) {
	c.Lock()
	c.stage = s
	c.Unlock()
}

type benchmarkStage string

const (
	stagePrepare    benchmarkStage = "prepare"
	stageBenchmark                 = "benchmark"
	stageCleanup                   = "cleanup"
	stageDone                      = "done"
	stageNotStarted                = ""
)

var benchmarkStages = []benchmarkStage{
	stagePrepare, stageBenchmark, stageCleanup,
}

func runClientBenchmark(ctx *cli.Context, b bench.Benchmark, cb *clientBenchmark) error {
	err := cb.waitForStage(stagePrepare)
	if err != nil {
		return err
	}
	cb.Lock()
	start := cb.info[stageBenchmark].start
	ctx2, cancel := context.WithCancel(cb.ctx)
	defer cancel()
	cb.Unlock()
	err = b.Prepare(ctx2)
	cb.stageDone(stagePrepare, err)
	if err != nil {
		return err
	}

	// Start after waiting a second or until we reached the start time.
	benchDur := ctx.Duration("duration")
	go func() {
		console.Infoln("等待中")
		// Wait for start signal
		select {
		case <-ctx2.Done():
			console.Infoln("已中止")
			return
		case <-start:
		}
		console.Infoln("已开始")
		// Finish after duration
		select {
		case <-ctx2.Done():
			console.Infoln("已中止")
			return
		case <-time.After(benchDur):
		}
		console.Infoln("停止中")
		// Stop the benchmark
		cancel()
	}()

	fileName := ctx.String("benchdata")
	cID := pRandASCII(6)
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"), cID)
	}

	ops, err := b.Start(ctx2, start)
	cb.Lock()
	cb.results = ops
	cb.Unlock()
	cb.stageDone(stageBenchmark, err)
	if err != nil {
		return err
	}
	ops.SetClientID(cID)
	ops.SortByStartTime()

	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		console.Error("无法写入基准测试数据:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "无法压缩基准测试数据到输出")

			defer enc.Close()
			err = ops.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "无法写入基准测试数据到输出")

			console.Infof("基准测试数据写入到了 %q\n", fileName+".csv.zst")
		}()
	}

	err = cb.waitForStage(stageCleanup)
	if err != nil {
		return err
	}
	if !ctx.Bool("keep-data") && !ctx.Bool("noclear") {
		console.Infoln("开始清理数据 ...")
		b.Cleanup(context.Background())
	}
	cb.stageDone(stageCleanup, nil)

	return nil
}

type runningProfiles struct {
	client *madmin.AdminClient
}

func startProfiling(ctx2 context.Context, ctx *cli.Context) (*runningProfiles, error) {
	prof := ctx.String("serverprof")
	if len(prof) == 0 {
		return nil, nil
	}
	var r runningProfiles
	r.client = newAdminClient(ctx)

	// Start profile
	_, cmdErr := r.client.StartProfiling(ctx2, madmin.ProfilerType(prof))
	if cmdErr != nil {
		return nil, cmdErr
	}
	console.Infoln("已成功启动了服务器分析.")
	return &r, nil
}

func (rp *runningProfiles) stop(ctx2 context.Context, ctx *cli.Context, fileName string) {
	if rp == nil || rp.client == nil {
		return
	}

	// Ask for profile data, which will come compressed with zip format
	zippedData, adminErr := rp.client.DownloadProfilingData(ctx2)
	fatalIf(probe.NewError(adminErr), "无法下载配置文件数据.")
	defer zippedData.Close()

	f, err := os.Create(fileName)
	if err != nil {
		console.Error("无法写入配置文件数据:", err)
		return
	}
	defer f.Close()

	// Copy zip content to target download file
	_, err = io.Copy(f, zippedData)
	if err != nil {
		console.Error("无法下载配置文件数据:", err)
		return
	}

	console.Infof("配置文件数据已成功下载为 %s\n", fileName)
}

func checkBenchmark(ctx *cli.Context) {
	profilerTypes := []madmin.ProfilerType{
		madmin.ProfilerCPU,
		madmin.ProfilerMEM,
		madmin.ProfilerBlock,
		madmin.ProfilerMutex,
		madmin.ProfilerTrace,
	}

	profs := strings.Split(ctx.String("serverprof"), ",")
	for _, profilerType := range profs {
		if len(profilerType) == 0 {
			continue
		}
		// Check if the provided profiler type is known and supported
		supportedProfiler := false
		for _, profiler := range profilerTypes {
			if profilerType == string(profiler) {
				supportedProfiler = true
				break
			}
		}
		if !supportedProfiler {
			fatalIf(errDummy(), "无法识别 Profiler 类型: %s . 可能的值是: %v.", profilerType, profilerTypes)
		}
	}
	if st := ctx.String("syncstart"); st != "" {
		t := parseLocalTime(st)
		if t.Before(time.Now()) {
			fatalIf(errDummy(), "syncstart 已通过: %v", t)
		}
	}
	if ctx.Bool("autoterm") {
		// TODO: autoterm cannot be used when in client/server mode
		if ctx.Duration("autoterm.dur") <= 0 {
			fatalIf(errDummy(), "autoterm.dur 的值不能是 0 或者负数")
		}
		if ctx.Float64("autoterm.pct") <= 0 {
			fatalIf(errDummy(), "autoterm.pct 的值不能是 0 或者负数")
		}
	}
}

// time format for start time.
const timeLayout = "15:04"

func parseLocalTime(s string) time.Time {
	t, err := time.ParseInLocation(timeLayout, s, time.Local)
	fatalIf(probe.NewError(err), "不能解析时间: %s", s)
	now := time.Now()
	y, m, d := now.Date()
	t = t.AddDate(y, int(m)-1, d-1)
	return t
}

// pRandASCII return pseudorandom ASCII string with length n.
// Should never be considered for true random data generation.
func pRandASCII(n int) string {
	const asciiLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	// Use a single seed.
	dst := make([]byte, n)
	var seed [8]byte

	// Get something random
	_, _ = rand.Read(seed[:])
	rnd := binary.LittleEndian.Uint32(seed[0:4])
	rnd2 := binary.LittleEndian.Uint32(seed[4:8])
	for i := range dst {
		dst[i] = asciiLetters[int(rnd>>16)%len(asciiLetters)]
		rnd ^= rnd2
		rnd *= 2654435761
	}
	return string(dst)
}
