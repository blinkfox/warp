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
	"fmt"
	"os"

	"github.com/minio/cli"
	"github.com/minio/minio/pkg/console"
)

// Collection of warp flags currently supported
var globalFlags = []cli.Flag{
	cli.BoolFlag{
		Name:   "quiet, q",
		Usage:  "禁用进度条",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "no-color",
		Usage: "禁用颜色主题",
	},
	cli.BoolFlag{
		Name:   "json",
		Usage:  "启用 JOSN 格式来输出数据",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "启用 debug 调试输出",
	},
	cli.BoolFlag{
		Name:  "insecure",
		Usage: "禁用 TLS 证书验证",
	},
	cli.BoolFlag{
		Name:  "autocompletion",
		Usage: "为 shell 安装自动补全",
	},
}

var profileFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "pprofdir",
		Usage:  "将配置信息写入到该文件夹",
		Value:  "pprof",
		Hidden: true,
	},

	cli.BoolFlag{
		Name:   "cpu",
		Usage:  "写入本地 CPU 配置信息",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "mem",
		Usage:  "写入本地内存分配的配置信息",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "block",
		Usage:  "写入本地 goroutine 块配置信息",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "mutex",
		Usage:  "写入互斥竞争的配置信息",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "threads",
		Usage:  "写入互斥竞争的配置信息",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:   "trace",
		Usage:  "写入本地执行时的跟踪信息",
		Hidden: true,
	},
}

// Set global states. NOTE: It is deliberately kept monolithic to ensure we dont miss out any flags.
func setGlobalsFromContext(ctx *cli.Context) error {
	quiet := ctx.IsSet("quiet")
	debug := ctx.IsSet("debug")
	json := ctx.IsSet("json")
	noColor := ctx.IsSet("no-color")
	setGlobals(quiet, debug, json, noColor)
	return nil
}

// Set global states. NOTE: It is deliberately kept monolithic to ensure we dont miss out any flags.
func setGlobals(quiet, debug, json, noColor bool) {
	globalQuiet = globalQuiet || quiet
	globalDebug = globalDebug || debug
	globalJSON = globalJSON || json
	globalNoColor = globalNoColor || noColor

	// Enable debug messages if requested.
	if globalDebug {
		console.DebugPrint = true
	}

	// Disable colorified messages if requested.
	if globalNoColor || globalQuiet {
		console.SetColorOff()
	}
}

// commandLine attempts to reconstruct the commandline.
func commandLine(ctx *cli.Context) string {
	s := os.Args[0] + " " + ctx.Command.Name
	for _, flag := range ctx.Command.Flags {
		val, err := flagToJSON(ctx, flag)
		if err != nil || val == "" {
			continue
		}
		name := flag.GetName()
		switch name {
		case "access-key", "secret-key":
			val = "*REDACTED*"
		}
		s += " --" + flag.GetName() + "=" + val
	}
	return s
}

// Flags common across all I/O commands such as cp, mirror, stat, pipe etc.
var ioFlags = []cli.Flag{
	cli.StringFlag{
		Name:   "host",
		Usage:  "主机 host 地址，可以将多个主机 host 指定为用逗号分割的列表.",
		EnvVar: appNameUC + "_HOST",
		Value:  "127.0.0.1:9000",
	},
	cli.StringFlag{
		Name:   "access-key",
		Usage:  "指定访问密钥 (access key)",
		EnvVar: appNameUC + "_ACCESS_KEY",
		Value:  "",
	},
	cli.StringFlag{
		Name:   "secret-key",
		Usage:  "指定私密密钥 (secret key)",
		EnvVar: appNameUC + "_SECRET_KEY",
		Value:  "",
	},
	cli.BoolFlag{
		Name:   "tls",
		Usage:  "使用 TLS (HTTPS) 进行传输",
		EnvVar: appNameUC + "_TLS",
	},
	cli.StringFlag{
		Name:   "region",
		Usage:  "指定自定义的区域 (region)",
		EnvVar: appNameUC + "_REGION",
	},
	cli.StringFlag{
		Name:   "signature",
		Usage:  "指定签名算法. 值可以是 S3V2, S3V4，默认使用 S3V4",
		Value:  "S3V4",
		Hidden: true,
	},
	cli.BoolFlag{
		Name:  "encrypt",
		Usage: "加密/解密对象 (使用带有随机密钥的服务器端加密)",
	},
	cli.StringFlag{
		Name:  "bucket",
		Value: appName + "-benchmark-bucket",
		Usage: "用于基准测试的存储桶. 该桶中的所有数据都将会被删除!",
	},
	cli.StringFlag{
		Name:  "host-select",
		Value: string(hostSelectTypeWeighed),
		Usage: fmt.Sprintf("主机 Host 的选择算法. 可以是 %q 或 %q", hostSelectTypeWeighed, hostSelectTypeRoundrobin),
	},
	cli.IntFlag{
		Name:  "concurrent",
		Value: 20,
		Usage: "运行基准测试时的并发请求数",
	},
	cli.BoolFlag{
		Name:  "noprefix",
		Usage: "不要为每个线程使用单独的前缀",
	},
	cli.BoolFlag{
		Name:  "disable-multipart",
		Usage: "禁用分片上传",
	},
	cli.BoolFlag{
		Name:  "md5",
		Usage: "上传过程中添加 MD5 值",
	},
	cli.StringFlag{
		Name:  "storage-class",
		Value: "",
		Usage: "指定自定义的存储类, 如: 'STANDARD' 或者 'REDUCED_REDUNDANCY'.",
	},
}
