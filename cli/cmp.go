/*
 * Warp (C) 2019 MinIO, Inc.
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
	"io"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var cmpFlags = []cli.Flag{}

var cmpCmd = cli.Command{
	Name:   "cmp",
	Usage:  "比较现有的基准测试数据",
	Action: mainCmp,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, analyzeFlags, cmpFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS] before-benchmark-data-file after-benchmark-data-file
  -> see https://github.com/minio/warp#comparing-benchmarks

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainAnalyze is the entry point for analyze command.
func mainCmp(ctx *cli.Context) error {
	checkAnalyze(ctx)
	checkCmp(ctx)
	args := ctx.Args()
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	readOps := func(s string) bench.Operations {
		f, err := os.Open(s)
		fatalIf(probe.NewError(err), "无法打开输入文件")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "无法读取输入文件")
		ops, err := bench.OperationsFromCSV(zstdDec, true, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "无法解析输入文件")
		return ops
	}
	printCompare(ctx, readOps(args[0]), readOps(args[1]))
	return nil
}

func printCompare(ctx *cli.Context, before, after bench.Operations) {
	var wrSegs io.Writer

	if fn := ctx.String("compare.out"); fn != "" {
		if fn == "-" {
			wrSegs = os.Stdout
		} else {
			f, err := os.Create(fn)
			fatalIf(probe.NewError(err), "无法创建分析后的输出文件")
			defer console.Println("聚合的数据已保存到", fn)
			defer f.Close()
			wrSegs = f
		}
	}
	_ = wrSegs
	isMultiOp := before.IsMixed()
	if isMultiOp != after.IsMixed() {
		console.Fatal("无法将多个请求操作与单个请求操作进行比较.")
	}
	timeDur := func(ops bench.Operations) time.Duration {
		start, end := ops.ActiveTimeRange(!isMultiOp)
		return end.Sub(start).Round(time.Second)
	}

	for _, typ := range before.OpTypes() {
		if wantOp := ctx.String("analyze.op"); wantOp != "" {
			if wantOp != typ {
				continue
			}
		}
		before := before.FilterByOp(typ)
		after := after.FilterByOp(typ)
		console.Println("-------------------")
		console.SetColor("Print", color.New(color.FgHiWhite))
		console.Println("请求操作:", typ)
		console.SetColor("Print", color.New(color.FgWhite))

		cmp, err := bench.Compare(before, after, analysisDur(ctx, before.Duration()), !isMultiOp)
		if err != nil {
			console.Println(err)
			continue
		}

		if len(before) != len(after) {
			console.Println("请求操作:", len(before), "->", len(after))
		}
		if before.Threads() != after.Threads() {
			console.Println("并发量:", before.Threads(), "->", after.Threads())
		}
		if len(before.Endpoints()) != len(after.Endpoints()) {
			console.Println("访问地址:", len(before.Endpoints()), "->", len(after.Endpoints()))
		}
		if !isMultiOp {
			if before.FirstObjPerOp() != after.FirstObjPerOp() {
				console.Println("每个请求操作的对象数:", before.FirstObjPerOp(), "->", after.FirstObjPerOp())
			}
		}
		if timeDur(before) != timeDur(after) {
			console.Println("持续时间:", timeDur(before), "->", timeDur(after))
		}
		console.Println("* 平均值:", cmp.Average)
		if cmp.TTFB != nil {
			console.Println("首个字节:", cmp.TTFB)
		}
		if !isMultiOp {
			console.SetColor("Print", color.New(color.FgWhite))
			console.Println("* 最快:", cmp.Fastest)
			console.Println("* 50% 中位数:", cmp.Median)
			console.Println("* 最慢:", cmp.Slowest)
		}
	}
}

func checkCmp(ctx *cli.Context) {
	if ctx.NArg() != 2 {
		console.Fatal("必须提供两个数据源")
	}
}
