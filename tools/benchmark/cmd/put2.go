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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/report"

	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"gopkg.in/cheggaaa/pb.v1"
	"strconv"
	"sync"
)

type put2configs struct {
	keySize int
	valSize int

	putTotal int
	putRate  int

	keySpaceSize int
	seqKeys      bool
	keyPrefix    string
	updateTotal  int
}

// putCmd represents the put command
var put2Cmd = &cobra.Command{
	Use:   "put2",
	Short: "Benchmark put",

	Run: put2Func,
}

var (
	put2config put2configs
)

func init() {
	RootCmd.AddCommand(put2Cmd)

	put2Cmd.Flags().IntVar(&put2config.keySize, "key-size", 8, "Key size of put request")
	put2Cmd.Flags().IntVar(&put2config.valSize, "val-size", 8, "Value size of put request")

	put2Cmd.Flags().IntVar(&put2config.putRate, "rate", 100, "Maximum puts per second (0 is no limit)")

	put2Cmd.Flags().IntVar(&put2config.putTotal, "total", 10000, "Total number of put requests")
	put2Cmd.Flags().IntVar(&put2config.keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	put2Cmd.Flags().BoolVar(&put2config.seqKeys, "sequential-keys", true, "Use sequential keys")

	put2Cmd.Flags().StringVar(&put2config.keyPrefix, "key-prefix", "", "Key prefix")
	put2Cmd.Flags().IntVar(&put2config.updateTotal, "update-total", 0, "Total number of update count. default is 0. sequential-keys,key-prefix needs to be set")

}

func put2Func(cmd *cobra.Command, args []string) {
	if put2config.keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", put2config.keySpaceSize)
		os.Exit(1)
	}

	requests := make(chan v3.Op, put2config.putRate)
	if putRate == 0 {
		putRate = math.MaxInt32
	}
	clients := mustCreateClients(totalClients, totalConns)
	k, v := make([]byte, put2config.keySize), string(mustRandBytes(put2config.valSize))

	bar = pb.New(put2config.putTotal + put2config.updateTotal)
	bar.Format("Bom !")
	bar.Start()

	r := newReport()
	wg.Add(put2config.putTotal + put2config.updateTotal)
	for i := range clients {
		go func(c *v3.Client) {
			for op := range requests {
				go func(c *v3.Client, op v3.Op) {
					defer wg.Done()
					st := time.Now()
					_, err := c.Do(context.Background(), op)
					r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
					bar.Increment()
				}(c, op)
			}
		}(clients[i])
	}

	go func() {
		for i := 0; i < put2config.putTotal; i++ {
			if put2config.keyPrefix != "" {
				k = []byte(put2config.keyPrefix + "/" + strconv.Itoa(i))
			} else {
				binary.PutVarint(k, int64(i%put2config.keySpaceSize))
			}
			requests <- v3.OpPut(string(k), v)
		}
		if put2config.keyPrefix != "" {
			//for i := 0; i < put2config.updateTotal; i++ {
			i := 0
			lock := sync.Mutex{}
			putTicker := time.NewTicker(time.Second / time.Duration(int64(put2config.putRate)))

			for range putTicker.C {
				go func() {
					lock.Lock()
					i++
					if i > put2config.updateTotal {
						putTicker.Stop()
					}
					lock.Unlock()
					k, v := []byte(put2config.keyPrefix+"/"+strconv.Itoa(rand.Intn(put2config.putTotal))), string(mustRandBytes(put2config.valSize))
					requests <- v3.OpPut(string(k), v)
				}()
			}
		}
		close(requests)
	}()

	rc := r.Run()
	wg.Wait()
	close(r.Results())
	bar.Finish()
	fmt.Println(<-rc)
}
