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
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	statFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 10000,
			Usage: "要上传的对象数. 四舍五入使其具有相等的并发对象数.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "1KB",
			Usage: "生成每个对象的大小. 可以是数字或 10KiB/MiB/GiB. 数字必须是 2^n 倍.",
		},
	}
)

var statCmd = cli.Command{
	Name:   "stat",
	Usage:  "基准测试中获取对象元数据信息 (stat) 的请求操作",
	Action: mainStat,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, statFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#stat

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainDelete is the entry point for get command.
func mainStat(ctx *cli.Context) error {
	checkStatSyntax(ctx)
	src := newGenSource(ctx)
	sse := newSSE(ctx)

	b := bench.Stat{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		StatOpts: minio.StatObjectOptions{
			ServerSideEncryption: sse,
		},
	}
	return runBench(ctx, &b)
}

func checkStatSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("命令中没有附带参数")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
