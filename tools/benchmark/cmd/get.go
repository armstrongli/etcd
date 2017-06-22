// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"math/rand"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/report"

	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"gopkg.in/cheggaaa/pb.v1"
	"strconv"
)

// putCmd represents the put command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Benchmark get",

	Run: getFunc,
}

var (
	keyTotal int

	//keyPrefix string
	getTotal int

	getQps int64
)

func init() {
	RootCmd.AddCommand(getCmd)

	getCmd.Flags().IntVar(&keyTotal, "key-total", 10000, "Total number of sequencial keys")
	getCmd.Flags().StringVar(&keyPrefix, "get-key-prefix", "", "Key prefix")
	getCmd.Flags().IntVar(&getTotal, "get-total", 0, "Total number of get count. default is 0. sequential-keys,key-prefix needs to be set")
	getCmd.Flags().Int64Var(&getQps, "qps", 100, `Requests count per second`)

}

func getFunc(cmd *cobra.Command, args []string) {
	requests := make(chan v3.Op, getQps)
	clients := mustCreateClients(totalClients, totalConns)

	bar = pb.New(getTotal)
	bar.Format("Bom !")
	bar.Start()

	wg.Add(getTotal)
	r := newReport()
	for i := range clients {
		go func(c *v3.Client) {
			for op := range requests {
				go func(ctx context.Context, c *v3.Client, ops v3.Op) {
					defer wg.Done()
					r.Results() <- sendGetReq(context.Background(), c, op)
					bar.Increment()
				}(context.Background(), c, op)
			}
		}(clients[i])
	}

	getTicker := time.NewTicker(time.Second / time.Duration(getQps))

	go func() {
		i := 0
		for range getTicker.C {
			k := keyPrefix + "/" + strconv.Itoa(rand.Intn(keyTotal))
			requests <- v3.OpGet(k)
			i++
			if i >= getTotal {
				break
			}
		}
	}()

	rc := r.Run()
	wg.Wait()
	close(requests)
	close(r.Results())
	bar.Finish()
	select {
	case <-rc:
	}
}

func sendGetReq(ctx context.Context, c *v3.Client, ops v3.Op) report.Result {
	st := time.Now()
	_, err := c.Do(ctx, ops)
	return report.Result{Err: err, Start: st, End: time.Now()}
}
