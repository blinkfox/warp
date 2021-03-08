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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/api"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

var analyzeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "analyze.dur",
		Value: "",
		Usage: "将分析拆分为该长度的持续时间. 可以是 '1s', '5s', '1m' 等.",
	},
	cli.StringFlag{
		Name:  "analyze.out",
		Value: "",
		Usage: "将聚合数据输出到文件",
	},
	cli.StringFlag{
		Name:  "analyze.op",
		Value: "",
		Usage: "指定某种操作的输出. 可以是 GET/PUT/DELETE 等.",
	},
	cli.StringFlag{
		Name:  "analyze.host",
		Value: "",
		Usage: "仅此主机 host 中的输出.",
	},
	cli.DurationFlag{
		Name:   "analyze.skip",
		Usage:  "分析数据时要跳过的附加持续时间.",
		Hidden: false,
		Value:  0,
	},
	cli.IntFlag{
		Name:   "analyze.limit",
		Usage:  "要加载进行分析的最大操作数.",
		Hidden: true,
		Value:  0,
	},
	cli.IntFlag{
		Name:   "analyze.offset",
		Usage:  "跳过指定数量的操作进行分析",
		Hidden: true,
		Value:  0,
	},
	cli.BoolFlag{
		Name:  "analyze.v",
		Usage: "显示其他分析数据.",
	},
	cli.StringFlag{
		Name:   serverFlagName,
		Usage:  "当运行基准测试时，在该 ip:port 上打开一个 web 服务，以让它持续运行.",
		Value:  "",
		Hidden: true,
	},
}

var analyzeCmd = cli.Command{
	Name:   "analyze",
	Usage:  "分析已有的基准测试数据",
	Action: mainAnalyze,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS] benchmark-data-file
  -> see https://github.com/minio/warp#analysis

Use - 作为输入从标准输入读取.

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainAnalyze is the entry point for analyze command.
func mainAnalyze(ctx *cli.Context) error {
	checkAnalyze(ctx)
	args := ctx.Args()
	if len(args) == 0 {
		console.Fatal("未提供基准测试数据的文件")
	}
	if len(args) > 1 {
		console.Fatal("只能提供一个基准文件")
	}
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	monitor := api.NewBenchmarkMonitor(ctx.String(serverFlagName))
	defer monitor.Done()
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	for _, arg := range args {
		var input io.Reader
		if arg == "-" {
			input = os.Stdin
		} else {
			f, err := os.Open(arg)
			fatalIf(probe.NewError(err), "无法打开输入文件")
			defer f.Close()
			input = f
		}
		err := zstdDec.Reset(input)
		fatalIf(probe.NewError(err), "无法读取输入")
		ops, err := bench.OperationsFromCSV(zstdDec, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "无法解析输入")

		printAnalysis(ctx, ops)
		monitor.OperationsReady(ops, strings.TrimSuffix(filepath.Base(arg), ".csv.zst"), commandLine(ctx))
	}
	return nil
}

func printMixedOpAnalysis(ctx *cli.Context, aggr aggregate.Aggregated, details bool) {
	console.SetColor("Print", color.New(color.FgWhite))
	console.Printf("混合的请求操作.")

	if aggr.MixedServerStats == nil {
		console.Errorln("没有混合统计")
	}
	for _, ops := range aggr.Operations {
		console.Println("")
		console.SetColor("Print", color.New(color.FgHiWhite))
		pct := 0.0
		if aggr.MixedServerStats.Operations > 0 {
			pct = 100.0 * float64(ops.Throughput.Operations) / float64(aggr.MixedServerStats.Operations)
		}
		duration := ops.EndTime.Sub(ops.StartTime).Truncate(time.Second)
		if !details {
			console.Printf("请求操作: %v, %d%%, 并发量: %d, 持续时间: %v.\n", ops.Type, int(pct+0.5), ops.Concurrency, duration)
		} else {
			console.Printf("请求操作: %v - 总计: %v, %.01f%%, 并发量: %d, 持续时间: %v, 开始时间 %v\n", ops.Type, ops.Throughput.Operations, pct, ops.Concurrency, duration, ops.StartTime.Truncate(time.Millisecond))
		}
		console.SetColor("Print", color.New(color.FgWhite))

		if ops.Skipped {
			console.Println("正在跳过", ops.Type, "样本太少，可靠的结果需要更长的基准运行时间.")
			continue
		}

		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("错误:", ops.Errors)
			if details {
				for _, err := range ops.FirstErrors {
					console.Println(err)
				}
			}
			console.SetColor("Print", color.New(color.FgWhite))
		}
		eps := ops.ThroughputByHost
		if len(eps) == 1 || !details {
			console.Println(" * 吞吐量:", ops.Throughput.StringDetails(details))
		}

		if len(eps) > 1 && details {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("\n主机吞吐量:")

			for ep, totals := range eps {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ": 平均值: ", totals.StringDetails(details), ".")
				if totals.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Print(" 错误: ", totals.Errors)
				}
				console.Println("")
			}
		}

		if details {
			printRequestAnalysis(ctx, ops, details)
			console.SetColor("Print", color.New(color.FgWhite))
		}
	}
	console.SetColor("Print", color.New(color.FgHiWhite))
	dur := time.Duration(aggr.MixedServerStats.MeasureDurationMillis) * time.Millisecond
	dur = dur.Round(time.Second)
	console.Printf("\n结果总计: %v 持续时间 %v.\n", aggr.MixedServerStats.StringDetails(details), dur)
	if aggr.MixedServerStats.Errors > 0 {
		console.SetColor("Print", color.New(color.FgHiRed))
		console.Print("总错误数:", aggr.MixedServerStats.Errors, ".\n")
	}
	console.SetColor("Print", color.New(color.FgWhite))
	if eps := aggr.MixedThroughputByHost; len(eps) > 1 && details {
		for ep, ops := range eps {
			console.Println(" * "+ep+":", ops.StringDetails(details))
		}
	}
}

func printAnalysis(ctx *cli.Context, o bench.Operations) {
	details := ctx.Bool("analyze.v")
	var wrSegs io.Writer
	prefiltered := false
	if fn := ctx.String("analyze.out"); fn != "" {
		if fn == "-" {
			wrSegs = os.Stdout
		} else {
			f, err := os.Create(fn)
			fatalIf(probe.NewError(err), "无法创建分析输出")
			defer console.Println("聚合数据保存到", fn)
			defer f.Close()
			wrSegs = f
		}
	}
	if onlyHost := ctx.String("analyze.host"); onlyHost != "" {
		o2 := o.FilterByEndpoint(onlyHost)
		if len(o2) == 0 {
			hosts := o.Endpoints()
			console.Println("找不到主机 host，有效的主机为:")
			for _, h := range hosts {
				console.Println("\t* %s", h)
			}
			return
		}
		prefiltered = true
		o = o2
	}

	if wantOp := ctx.String("analyze.op"); wantOp != "" {
		prefiltered = prefiltered || o.IsMixed()
		o = o.FilterByOp(wantOp)
	}
	durFn := func(total time.Duration) time.Duration {
		if total <= 0 {
			return 0
		}
		return analysisDur(ctx, total)
	}
	aggr := aggregate.Aggregate(o, aggregate.Options{
		Prefiltered: prefiltered,
		DurFunc:     durFn,
		SkipDur:     ctx.Duration("analyze.skip"),
	})
	if wrSegs != nil {
		for _, ops := range aggr.Operations {
			writeSegs(ctx, wrSegs, o.FilterByOp(ops.Type), aggr.Mixed || prefiltered, details)
		}
	}

	if globalJSON {
		b, err := json.MarshalIndent(aggr, "", "  ")
		fatalIf(probe.NewError(err), "无法组织数据.")
		if err != nil {
			console.Errorln(err)
		}
		os.Stdout.Write(b)
		return
	}

	if aggr.Mixed {
		printMixedOpAnalysis(ctx, aggr, details)
		return
	}

	for _, ops := range aggr.Operations {
		typ := ops.Type
		console.Println("\n----------------------------------------")

		opo := ops.ObjectsPerOperation
		console.SetColor("Print", color.New(color.FgHiWhite))
		hostsString := ""
		if ops.Hosts > 1 {
			hostsString = fmt.Sprintf(" 主机: %d.", ops.Hosts)
		}
		if ops.Clients > 1 {
			hostsString = fmt.Sprintf("%s Warp 实例: %d.", hostsString, ops.Clients)
		}
		if opo > 1 {
			if details {
				console.Printf("请求操作: %v (%d). 每次操作的对象数: %d. 并发量: %d.%s\n", typ, ops.N, opo, ops.Concurrency, hostsString)
			} else {
				console.Printf("请求操作: %v\n", typ)
			}
		} else {
			if details {
				console.Printf("请求操作: %v (%d). 并发量: %d.%s\n", typ, ops.N, ops.Concurrency, hostsString)
			} else {
				console.Printf("请求操作: %v\n", typ)
			}
		}
		if ops.Errors > 0 {
			console.SetColor("Print", color.New(color.FgHiRed))
			console.Println("错误:", ops.Errors)
			if details {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Println("首个错误:")
				for _, err := range ops.FirstErrors {
					console.Println(" *", err)
				}
				console.Println("")
			}
		}

		if ops.Skipped {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("正在跳过", typ, "样本太少，可靠的结果需要更长的基准运行时间.")
			continue
		}

		if details {
			printRequestAnalysis(ctx, ops, details)
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\n吞吐量:")
		}
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println("* 平均值:", ops.Throughput.StringDetails(details))

		if eps := ops.ThroughputByHost; len(eps) > 1 {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\n主机吞吐量:")

			for ep, ops := range eps {
				console.SetColor("Print", color.New(color.FgWhite))
				console.Print(" * ", ep, ":")
				if !details {
					console.Print(" 平均值: ", ops.StringDetails(details), "\n")
				} else {
					console.Print("\n")
				}
				if ops.Errors > 0 {
					console.SetColor("Print", color.New(color.FgHiRed))
					console.Println("错误:", ops.Errors)
				}
				if details {
					seg := ops.Segmented
					console.SetColor("Print", color.New(color.FgWhite))
					if seg == nil || len(seg.Segments) <= 1 {
						console.Println("正在跳过", typ, "主机:", ep, " - 样本太少，可靠的结果需要更长的基准运行时间.")
						continue
					}
					console.SetColor("Print", color.New(color.FgWhite))
					console.Println("\t- 平均值: ", ops.StringDetails(false))
					console.Println("\t- 最快的:", aggregate.BPSorOPS(seg.FastestBPS, seg.FastestOPS))
					console.Println("\t- 中位数:", aggregate.BPSorOPS(seg.MedianBPS, seg.MedianOPS))
					console.Println("\t- 最慢的:", aggregate.BPSorOPS(seg.SlowestBPS, seg.SlowestOPS))
				}
			}
		}
		segs := ops.Throughput.Segmented
		dur := time.Millisecond * time.Duration(segs.SegmentDurationMillis)
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Print("\n吞吐量, 分成 ", len(segs.Segments), " x ", dur, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))
		console.Println(" * 最快的:", aggregate.SegmentSmall{BPS: segs.FastestBPS, OPS: segs.FastestOPS, Start: segs.FastestStart}.StringLong(dur, details))
		console.Println(" * 中位数:", aggregate.SegmentSmall{BPS: segs.MedianBPS, OPS: segs.MedianOPS, Start: segs.MedianStart}.StringLong(dur, details))
		console.Println(" * 最慢的:", aggregate.SegmentSmall{BPS: segs.SlowestBPS, OPS: segs.SlowestOPS, Start: segs.SlowestStart}.StringLong(dur, details))
	}
}

func writeSegs(ctx *cli.Context, wrSegs io.Writer, ops bench.Operations, allThreads, details bool) {
	if wrSegs == nil {
		return
	}
	totalDur := ops.Duration()
	segs := ops.Segment(bench.SegmentOptions{
		From:           time.Time{},
		PerSegDuration: analysisDur(ctx, totalDur),
		AllThreads:     allThreads && !ops.HasError(),
	})

	segs.SortByTime()
	err := segs.CSV(wrSegs)
	errorIf(probe.NewError(err), "写入分析时出错")

	// Write segments per endpoint
	eps := ops.Endpoints()
	if details && len(eps) > 1 {
		for _, ep := range eps {
			ops := ops.FilterByEndpoint(ep)
			segs := ops.Segment(bench.SegmentOptions{
				From:           time.Time{},
				PerSegDuration: analysisDur(ctx, totalDur),
				AllThreads:     false,
			})
			if len(segs) <= 1 {
				continue
			}
			totals := ops.Total(false)
			if totals.TotalBytes > 0 {
				segs.SortByThroughput()
			} else {
				segs.SortByObjsPerSec()
			}
			segs.SortByTime()
			err := segs.CSV(wrSegs)
			errorIf(probe.NewError(err), "写入分析时出错")
		}
	}
}

func printRequestAnalysis(ctx *cli.Context, ops aggregate.Operation, details bool) {
	console.SetColor("Print", color.New(color.FgHiWhite))

	if ops.SingleSizedRequests != nil {
		reqs := *ops.SingleSizedRequests
		// Single type, require one operation per thread.

		console.Print("\nconsidered 请求: ", reqs.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		if reqs.Skipped {
			fmt.Println(reqs)
			console.Println("请求数量不足")
			return
		}

		console.Print(
			" * 平均: ", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
			", 50%: ", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
			", 90%: ", time.Duration(reqs.Dur90Millis)*time.Millisecond,
			", 99%: ", time.Duration(reqs.Dur99Millis)*time.Millisecond,
			", 最快: ", time.Duration(reqs.FastestMillis)*time.Millisecond,
			", 最慢: ", time.Duration(reqs.SlowestMillis)*time.Millisecond,
			"\n")

		if reqs.FirstByte != nil {
			console.Println(" * First Byte:", reqs.FirstByte)
		}

		if reqs.FirstAccess != nil {
			reqs := reqs.FirstAccess
			console.Print(
				" * 首次访问: 平均: ", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
				", 50%: ", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
				", 90%: ", time.Duration(reqs.Dur90Millis)*time.Millisecond,
				", 99%: ", time.Duration(reqs.Dur99Millis)*time.Millisecond,
				", 最快: ", time.Duration(reqs.FastestMillis)*time.Millisecond,
				", 最慢: ", time.Duration(reqs.SlowestMillis)*time.Millisecond,
				"\n")
			if reqs.FirstByte != nil {
				console.Print(" * 首次访问 TTFB: ", reqs.FirstByte)
			}
			console.Println("")
		}

		if eps := reqs.ByHost; len(eps) > 1 && details {
			console.SetColor("Print", color.New(color.FgHiWhite))
			console.Println("\n主机请求:")

			for ep, reqs := range eps {
				if reqs.Requests <= 1 {
					continue
				}
				console.SetColor("Print", color.New(color.FgWhite))
				console.Println(" *", ep, "-", reqs.Requests, "请求量:",
					"\n\t- 平均:", time.Duration(reqs.DurAvgMillis)*time.Millisecond,
					"最快:", time.Duration(reqs.FastestMillis)*time.Millisecond,
					"最慢:", time.Duration(reqs.SlowestMillis)*time.Millisecond,
					"50%:", time.Duration(reqs.DurMedianMillis)*time.Millisecond,
					"90%:", time.Duration(reqs.Dur90Millis)*time.Millisecond)
				if reqs.FirstByte != nil {
					console.Println("\t- 首个字节:", reqs.FirstByte)
				}
			}
		}
		return
	}

	// Multi sized
	if ops.MultiSizedRequests == nil {
		console.Fatalln("找不到 single-sized 或者 multi-sized 的请求")
	}
	reqs := *ops.MultiSizedRequests
	console.Print("\nconsidered 请求: ", reqs.Requests, ". 多种大小, 平均 ", reqs.AvgObjSize, " 字节:\n")
	console.SetColor("Print", color.New(color.FgWhite))

	if reqs.Skipped {
		console.Println("请求数量不足")
	}

	sizes := reqs.BySize
	for _, s := range sizes {

		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Print("\nRequest size ", s.MinSizeString, " -> ", s.MaxSizeString, ". Requests - ", s.Requests, ":\n")
		console.SetColor("Print", color.New(color.FgWhite))

		console.Print(""+
			" * 吞吐量: 平均: ", bench.Throughput(s.BpsAverage),
			", 50%: ", bench.Throughput(s.BpsMedian),
			", 90%: ", bench.Throughput(s.Bps90),
			", 99%: ", bench.Throughput(s.Bps99),
			", 最快: ", bench.Throughput(s.BpsFastest),
			", 最慢: ", bench.Throughput(s.BpsSlowest),
			"\n")

		if s.FirstByte != nil {
			console.Println(" * 首个字节:", s.FirstByte)
		}

		if s.FirstAccess != nil {
			s := s.FirstAccess
			console.Print(""+
				" * 首次访问: 平均: ", bench.Throughput(s.BpsAverage),
				", 50%: ", bench.Throughput(s.BpsMedian),
				", 90%: ", bench.Throughput(s.Bps90),
				", 99%: ", bench.Throughput(s.Bps99),
				", 最快: ", bench.Throughput(s.BpsFastest),
				", 最慢: ", bench.Throughput(s.BpsSlowest),
				"\n")
			if s.FirstByte != nil {
				console.Print(" * 首次访问 TTFB: ", s.FirstByte, "\n")
			}
		}

	}
	if eps := reqs.ByHost; len(eps) > 1 && details {
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("\n主机请求:")

		for ep, s := range eps {
			if s.Requests <= 1 {
				continue
			}
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println(" *", ep, "-", s.Requests, "requests:",
				"\n\t- 平均:", bench.Throughput(s.BpsAverage),
				"最快:", bench.Throughput(s.BpsFastest),
				"最慢:", bench.Throughput(s.BpsSlowest),
				"50%:", bench.Throughput(s.BpsMedian),
				"90%:", bench.Throughput(s.Bps90))
			if s.FirstByte != nil {
				console.Println(" * 首个字节:", s.FirstByte)
			}
		}
	}
}

// analysisDur returns the analysis duration or 0 if un-parsable.
func analysisDur(ctx *cli.Context, total time.Duration) time.Duration {
	dur := ctx.String("analyze.dur")
	if dur == "" {
		if total == 0 {
			return 0
		}
		// Find appropriate duration
		// We want the smallest segmentation duration that produces at most this number of segments.
		const wantAtMost = 400

		// Standard durations to try:
		stdDurations := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 3 * time.Hour}
		for _, d := range stdDurations {
			dur = d.String()
			if total/d <= wantAtMost {
				break
			}
		}
	}
	d, err := time.ParseDuration(dur)
	fatalIf(probe.NewError(err), "无效的 -analyze.dur 值")
	return d
}

func checkAnalyze(ctx *cli.Context) {
	if analysisDur(ctx, time.Minute) == 0 {
		err := errors.New("-analyze.dur 的值不能是 0")
		fatal(probe.NewError(err), "无效的 -analyze.dur 值")
	}
}
