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
	putFlags = []cli.Flag{
		cli.StringFlag{
			Name:  "obj.size",
			Value: "10MiB",
			Usage: "生成每个对象的大小. 可以是数字或 10KiB/MiB/GiB. 数字必须是 2^n 倍.",
		},
	}
)

// Put command.
var putCmd = cli.Command{
	Name:   "put",
	Usage:  "基准测试中获取对象 (put) 的请求操作",
	Action: mainPut,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, putFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#put

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainPut is the entry point for cp command.
func mainPut(ctx *cli.Context) error {
	checkPutSyntax(ctx)
	src := newGenSource(ctx)
	b := bench.Put{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
	}
	return runBench(ctx, &b)
}

// putOpts retrieves put options from the context.
func putOpts(ctx *cli.Context) minio.PutObjectOptions {
	return minio.PutObjectOptions{
		ServerSideEncryption: newSSE(ctx),
		DisableMultipart:     ctx.Bool("disable-multipart"),
		SendContentMd5:       ctx.Bool("md5"),
		StorageClass:         ctx.String("storage-class"),
	}
}

func checkPutSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("命令中没有附带参数")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
