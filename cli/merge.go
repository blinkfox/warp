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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var mergeFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "benchdata",
		Value: "",
		Usage: "将合并的数据输出到该文件. 默认会生成唯一的文件名.",
	},
}

var mergeCmd = cli.Command{
	Name:   "merge",
	Usage:  "合并现有的基准测试数据",
	Action: mainMerge,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, mergeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS] benchmark-data-file1 benchmark-data-file2 ... 
  -> see https://github.com/minio/warp#merging-benchmarks

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainAnalyze is the entry point for analyze command.
func mainMerge(ctx *cli.Context) error {
	checkMerge(ctx)
	args := ctx.Args()
	if len(args) <= 1 {
		console.Fatal("必须提供两个或多个基准测试的数据文件")
	}
	var zstdDec, _ = zstd.NewReader(nil)
	defer zstdDec.Close()
	var allOps bench.Operations
	threads := uint16(0)
	log := console.Printf
	if globalQuiet {
		log = nil
	}
	for _, arg := range args {
		f, err := os.Open(arg)
		fatalIf(probe.NewError(err), "无法打开输入文件")
		defer f.Close()
		err = zstdDec.Reset(f)
		fatalIf(probe.NewError(err), "无法解压缩输入文件")
		ops, err := bench.OperationsFromCSV(zstdDec, false, ctx.Int("analyze.offset"), ctx.Int("analyze.limit"), log)
		fatalIf(probe.NewError(err), "无法解析输入文件")

		threads = ops.OffsetThreads(threads)
		allOps = append(allOps, ops...)
	}
	if len(allOps) == 0 {
		return errors.New("基准测试文件中没有任何数据")
	}
	fileName := ctx.String("benchdata")
	if fileName == "" {
		fileName = fmt.Sprintf("%s-%s-%s", appName, ctx.Command.Name, time.Now().Format("2006-01-02[150405]"))
	}
	allOps.SortByStartTime()
	f, err := os.Create(fileName + ".csv.zst")
	if err != nil {
		console.Error("无法写入基准测试数据:", err)
	} else {
		func() {
			defer f.Close()
			enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			fatalIf(probe.NewError(err), "无法压缩基准测试数据到输出")

			defer enc.Close()
			err = allOps.CSV(enc, commandLine(ctx))
			fatalIf(probe.NewError(err), "无法写入基准测试数据到输出")

			console.Infof("基准测试数据写入到了 %q\n", fileName+".csv.zst")
		}()
	}
	for typ, ops := range allOps.ByOp() {
		start, end := ops.ActiveTimeRange(true)
		if !start.Before(end) {
			console.Errorf("类型 %v 中没有重叠项", typ)
		}
	}
	return nil
}

func checkMerge(ctx *cli.Context) {
}
