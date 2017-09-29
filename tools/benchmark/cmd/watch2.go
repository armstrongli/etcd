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
	"fmt"
	"os"
	"sync"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// watchCmd represents the watch command
var watch2Cmd = &cobra.Command{
	Use:   "watch2",
	Short: "Benchmark watch",
	Long: `Benchmark watch tests the performance of processing watch requests and 
sending events to watchers. It tests the sending performance by 
changing the value of the watched keys with concurrent put 
requests.

During the test, each watcher watches (--total/--watchers) keys 

(a watcher might watch on the same key multiple times if 
--watched-key-total is small).

Each key is watched by (--total/--watched-key-total) watchers.
`,
	Run: watch2Func,
}

var (
	keyPrefix     string
	watchDuration time.Duration
)

func init() {
	RootCmd.AddCommand(watch2Cmd)
	watch2Cmd.Flags().IntVar(&watchTotalStreams, "watchers", 10000, "Total number of watchers")
	watch2Cmd.Flags().IntVar(&watchTotal, "total", 100000, "Total number of watch requests")
	watch2Cmd.Flags().IntVar(&watchedKeyTotal, "watched-key-total", 10000, "Total number of keys to be watched")

	watch2Cmd.Flags().IntVar(&watchPutRate, "put-rate", 100, "Number of keys to put per second")
	watch2Cmd.Flags().IntVar(&watchPutTotal, "put-total", 10000, "Number of put requests")

	watch2Cmd.Flags().IntVar(&watchKeySize, "key-size", 32, "Key size of watch request")
	watch2Cmd.Flags().IntVar(&watchKeySpaceSize, "key-space-size", 1, "Maximum possible keys")
	watch2Cmd.Flags().BoolVar(&watchSeqKeys, "sequential-keys", false, "Use sequential keys")
	watch2Cmd.Flags().StringVar(&keyPrefix, "key-prefix", "", "The prefix of sequential keys")

	watch2Cmd.Flags().DurationVar(&watchDuration, "watch-duration", time.Minute, "Watch duration")
}

func watch2Func(cmd *cobra.Command, args []string) {
	if watchKeySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", watchKeySpaceSize)
		os.Exit(1)
	}

	clients := mustCreateClients(totalClients, totalConns)

	watcherChans := []<-chan v3.WatchResponse{}

	for i := 0; i < watchTotalStreams; i++ {
		c := clients[i%len(clients)].Watch(context.WithValue(context.Background(), "K", i), keyPrefix, v3.WithPrefix())
		watcherChans = append(watcherChans, c)
	}

	wg := sync.WaitGroup{}
	wg.Add(watchTotalStreams)
	for i := range watcherChans {
		go func(i int) {
			c := watcherChans[i]
			for r := range c {
				ks := ""
				for _, e := range r.Events {
					ks += string(e.Kv.Key)
				}
				fmt.Println(i, ks)
			}
			fmt.Println(i, "closed")

			// restart new watcher in case some watcher stopped by etcd server
			cNew := clients[i%len(clients)].Watch(context.WithValue(context.Background(), "K", i), keyPrefix, v3.WithPrefix())
			watcherChans[i] = cNew

			// todo
			//wg.Done()
		}(i)
	}
	watchSign := make(chan int)
	go func() {
		wg.Wait()
		watchSign <- 1
	}()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), watchDuration)
	defer func() { cancel() }()
	select {
	case <-timeoutCtx.Done():
	case <-watchSign:
	}

}
