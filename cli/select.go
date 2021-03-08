/*
 * Warp (C) 2020 MinIO, Inc.
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
	"github.com/minio/warp/pkg/bench"
)

var (
	selectFlags = []cli.Flag{
		cli.IntFlag{
			Name:  "objects",
			Value: 2500,
			Usage: "要上传的对象数.",
		},
		cli.StringFlag{
			Name:  "obj.size",
			Value: "10MiB",
			Usage: "生成每个对象的大小. 可以是数字或 10KiB/MiB/GiB. 数字必须是 2^n 倍.",
		},
		cli.StringFlag{
			Name:  "query",
			Value: "select * from s3object",
			Usage: "select 查询的表达式",
		},
	}
)

var selectCmd = cli.Command{
	Name:   "select",
	Usage:  "基准测试中选择对象 (select) 的请求操作",
	Action: mainSelect,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, ioFlags, selectFlags, genFlags, benchFlags, analyzeFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainSelect is the entry point for select command.
func mainSelect(ctx *cli.Context) error {
	checkSelectSyntax(ctx)
	src := newGenSourceCSV(ctx)
	sse := newSSE(ctx)
	b := bench.Select{
		Common: bench.Common{
			Client:      newClient(ctx),
			Concurrency: ctx.Int("concurrent"),
			Source:      src,
			Bucket:      ctx.String("bucket"),
			Location:    "",
			PutOpts:     putOpts(ctx),
		},
		CreateObjects: ctx.Int("objects"),
		SelectOpts: minio.SelectObjectOptions{
			Expression:     ctx.String("query"),
			ExpressionType: minio.QueryExpressionTypeSQL,
			// Set any encryption headers
			ServerSideEncryption: sse,
			// TODO: support all variations including, json/parquet
			InputSerialization: minio.SelectObjectInputSerialization{
				CSV: &minio.CSVInputOptions{
					RecordDelimiter: "\n",
					FieldDelimiter:  ",",
					FileHeaderInfo:  minio.CSVFileHeaderInfoUse,
				},
			},
			OutputSerialization: minio.SelectObjectOutputSerialization{
				CSV: &minio.CSVOutputOptions{
					RecordDelimiter: "\n",
					FieldDelimiter:  ",",
				},
			},
		},
	}
	return runBench(ctx, &b)
}

func checkSelectSyntax(ctx *cli.Context) {
	checkAnalyze(ctx)
	checkBenchmark(ctx)
}
