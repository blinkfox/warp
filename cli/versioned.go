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
	"net/http"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	versionedFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 250,
			Usage: "要上传的对象数.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "10MiB",
			Usage: "生成每个对象的大小. 可以是数字或 10KiB/MiB/GiB. 数字必须是 2^n 倍.",
		},
		cli.Float64Flag{
			Name:  "get-distrib",
			Usage: "GET 请求操作权重量.",
			Value: 45,
		},
		cli.Float64Flag{
			Name:  "stat-distrib",
			Usage: "STAT 请求操作权重量.",
			Value: 30,
		},
		cli.Float64Flag{
			Name:  "put-distrib",
			Usage: "PUT 请求操作权重量.",
			Value: 15,
		},
		cli.Float64Flag{
			Name:  "delete-distrib",
			Usage: "DELETE 请求操作权重量. 须小于等于 PUT 请求权重量.",
			Value: 10,
		},
	}
)

var versionedCmd = cli.Command{
	Name:   "versioned",
	Usage:  "混合对象版本 (versioned) 功能请求操作的基准测试",
	Action: mainVersioned,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, versionedFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#mixed

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainVersioned is the entry point for mixed command.
func mainVersioned(ctx *cli.Context) error {
	checkVersionedSyntax(ctx)
	src := newGenSource(ctx)
	sse := newSSE(ctx)
	dist := bench.VersionedDistribution{
		Distribution: map[string]float64{
			http.MethodGet:    ctx.Float64("get-distrib"),
			"STAT":            ctx.Float64("stat-distrib"),
			http.MethodPut:    ctx.Float64("put-distrib"),
			http.MethodDelete: ctx.Float64("delete-distrib"),
		},
	}
	err := dist.Generate(ctx.Int("objects") * 2)
	fatalIf(probe.NewError(err), "无效的请求操作分配比例")
	b := bench.Versioned{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		GetOpts:       minio.GetObjectOptions{ServerSideEncryption: sse},
		StatOpts: minio.StatObjectOptions{
			ServerSideEncryption: sse,
		},
		Dist: &dist,
	}
	return runBench(ctx, &b)
}

func checkVersionedSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("命令中没有附带参数")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
