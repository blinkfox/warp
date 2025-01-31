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
	"strconv"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
)

var (
	clientFlags = []cli.Flag{}
)

// Put command.
var clientCmd = cli.Command{
	Name:   "client",
	Usage:  "以客户端模式运行 warp，接受连接来运行基准测试",
	Action: mainClient,
	Before: setGlobalsFromContext,
	Flags:  combineFlags(globalFlags, clientFlags),
	CustomHelpTemplate: `名称:
  {{.HelpName}} - {{.Usage}}

使用:
  {{.HelpName}} [FLAGS] [listen address]
  -> see https://github.com/minio/warp#multiple-hosts

参数:
  {{range .VisibleFlags}}{{.}}
  {{end}}

示例:
  1. 监听 ip 是 192.168.1.101 下的 '6001' 端口:
     {{.Prompt}} {{.HelpName}} 192.168.1.101:6001
 `,
}

const warpServerDefaultPort = 7761

// mainPut is the entry point for cp command.
func mainClient(ctx *cli.Context) error {
	checkClientSyntax(ctx)
	addr := ":" + strconv.Itoa(warpServerDefaultPort)
	switch ctx.NArg() {
	case 1:
		addr = ctx.Args()[0]
		if !strings.Contains(addr, ":") {
			addr += ":" + strconv.Itoa(warpServerDefaultPort)
		}
	case 0:
	default:
		fatal(errInvalidArgument(), "参数太多")
	}
	http.HandleFunc("/ws", serveWs)
	console.Infoln("正在监听", addr)
	fatalIf(probe.NewError(http.ListenAndServe(addr, nil)), "无法启动客户端")
	return nil
}

func checkClientSyntax(ctx *cli.Context) {
}
