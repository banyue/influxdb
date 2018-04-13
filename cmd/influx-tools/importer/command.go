package importer

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdin  io.Reader
	Logger *zap.Logger
	server server.Interface

	configPath      string
	database        string
	retentionPolicy string
	shardDuration   time.Duration
	buildTSI        bool
	replace         bool
	drop            bool

	cpuProfile string
	memProfile string
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
		server: server,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	i := NewImporter(cmd.server.MetaClient(), cmd.database, cmd.server.TSDBConfig().Dir, cmd.buildTSI, cmd.Logger)

	reader := binary.NewReader(cmd.Stdin, line.NewWriter(cmd.Stderr))
	_, err = reader.ReadHeader()
	if err != nil {
		return err
	}

	if cmd.drop {
		err = i.DropDatabase()
		if err != nil {
			return err
		}
	}
	err = i.CreateDatabase(&meta.RetentionPolicySpec{Name: cmd.retentionPolicy, ShardGroupDuration: cmd.shardDuration}, cmd.replace)
	if err != nil {
		return err
	}

	var bh *binary.BucketHeader
	var sh *binary.SeriesHeader
	var next bool

	for bh, err = reader.NextBucket(); (bh != nil) && (err == nil); bh, err = reader.NextBucket() {
		fmt.Printf("Read bucket header %v\n", bh)
		err = i.StartShardGroup(bh.Start, bh.End)
		if err != nil {
			return err
		}
		defer i.CloseShardGroup()

		err := i.StartSeriesFile()
		if err != nil {
			return err
		}
		defer i.CloseSeriesFile()

		reportTime := time.Now()
		count := 0
		series := 0
		for sh, err = reader.NextSeries(); (sh != nil) && (err == nil); sh, err = reader.NextSeries() {
			series++
			i.AddSeries(sh.SeriesKey)
			pr := reader.Points()
			seriesFieldKey := tsm1.SeriesFieldKeyBytes(string(sh.SeriesKey), string(sh.Field))

			for next, err = pr.Next(); next && (err == nil); next, err = pr.Next() {
				err = i.Write(seriesFieldKey, pr.Values())
				count += len(pr.Values())
				if time.Now().Unix() > reportTime.Unix()+60 {
					fmt.Printf("Read %v series, %v values\n", series, count)
					reportTime = time.Now()
				}
				if err != nil {
					return err
				}
			}
			if err != nil {
				return err
			}
		}

		fmt.Printf("Finished shard with a total of %v series, %v values\n", series, count)
		err = i.CloseShardGroup()
		if err != nil {
			return err
		}

		err = i.CloseSeriesFile()
		if err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.retentionPolicy, "rp", "", "Retention policy")
	fs.DurationVar(&cmd.shardDuration, "shard-duration", time.Hour*24*7, "Retention policy shard duration")
	fs.BoolVar(&cmd.buildTSI, "build-tsi", false, "Build the on disk TSI")
	fs.BoolVar(&cmd.replace, "replace", false, "Enables replacing an existing retention policy")
	fs.BoolVar(&cmd.drop, "drop", false, "Drop any existing database before importing")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	return nil
}
