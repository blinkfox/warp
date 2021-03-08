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
	"github.com/minio/minio/pkg/console"
	"github.com/minio/warp/pkg/bench"
)

var (
	deleteFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 25000,
			Usage: "要上传的对象数.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "1KiB",
			Usage: "生成每个对象的大小. 可以是数字或 10KiB/MiB/GiB. 数字必须是 2^n 倍.",
		},
		cli.IntFlag{
			Name:  "batch",
			Value: 100,
			Usage: "每批的删除请求操作数.",
		},
	}
)

var deleteCmd = cli.Command{
	Name:   "delete",
	Usage:  "基准测试中删除对象 (delete) 的请求操作",
	Action: mainDelete,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, deleteFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

  当所有对象都被删除了或者达到了指定的 -duration 持续时间，基准测试就会结束. 
使用:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#delete

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainDelete is the entry point for get command.
func mainDelete(ctx *cli.Context) error {
	checkDeleteSyntax(ctx)
	src := newGenSource(ctx)

	b := bench.Delete{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		BatchSize:     ctx.Int("batch"),
	}
	return runBench(ctx, &b)
}

func checkDeleteSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("命令中没有附带参数")
	}
	checkAnalyze(ctx)
	checkBenchmark(ctx)
	if ctx.Int("batch") < 1 {
		console.Fatal("批量大小必须大于等于 1")
	}
	wantO := ctx.Int("batch") * ctx.Int("concurrent") * 4
	if ctx.Int("objects") < wantO {
		console.Fatalf("对象太少: 请使用 --batch 和 --concurrent 参数进行设置, 有效的基准测试，至少需要 %d 个对象数. 可以使用 --objects=%d 来指定", wantO, wantO)
	}
}
