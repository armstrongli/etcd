package cmd

import (
	"context"
	"math/rand"
	"strconv"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/report"
	"github.com/spf13/cobra"
	pb "gopkg.in/cheggaaa/pb.v1"
)

// putCmd represents the put command
var getCmd = &cobra.Command{
	Use:   "get2",
	Short: "Benchmark get",

	Run: get2Func,
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

func get2Func(cmd *cobra.Command, args []string) {
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
