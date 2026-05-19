package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/sirupsen/logrus"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// hotWorkload is the per-iteration body of a hot-concurrent sweep.
// Returning an error increments the error counter; the iteration's
// elapsed time is discarded.
type hotWorkload func(rng *rand.Rand) error

// hotConcurrentResult is the same shape as coldConcurrentResult, but
// without the per-iteration open/close — the hot store is a single
// long-lived handle shared across workers.
type hotConcurrentResult struct {
	stats     latencyStats
	totalErrs int
	durs      []time.Duration
}

// runHotConcurrent fans out workers goroutines, each running
// itersPerWorker iterations of `op`. Workers get independent RNG
// streams so they don't synchronize on a shared source. Wall-clock
// aggregate ops/sec is computed from successful iterations only.
func runHotConcurrent(
	workers, itersPerWorker int,
	baseSeed int64,
	op hotWorkload,
) hotConcurrentResult {
	type workerResult struct {
		durs []time.Duration
		errs int
	}
	results := make([]workerResult, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	tStart := time.Now()
	for wID := 0; wID < workers; wID++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(
				uint64(baseSeed)+uint64(id),
				uint64(baseSeed*7919)+uint64(id),
			))
			durs := make([]time.Duration, 0, itersPerWorker)
			var errs int
			for i := 0; i < itersPerWorker; i++ {
				t0 := time.Now()
				err := op(rng)
				d := time.Since(t0)
				if err != nil {
					errs++
					continue
				}
				durs = append(durs, d)
			}
			results[id] = workerResult{durs: durs, errs: errs}
		}(wID)
	}
	wg.Wait()
	wall := time.Since(tStart)

	all := make([]time.Duration, 0, workers*itersPerWorker)
	var totalErrs int
	for _, r := range results {
		all = append(all, r.durs...)
		totalErrs += r.errs
	}
	stats := computeStats(all)
	stats.opsPerSec = float64(len(all)) / wall.Seconds()
	return hotConcurrentResult{stats: stats, totalErrs: totalErrs, durs: all}
}

// printSweepHeader emits the standard sweep summary header for stdout.
func printSweepHeader(scenarioCol string) {
	fmt.Printf("\n%-14s %-9s %-7s %-9s %-9s %-9s %-10s\n",
		scenarioCol, "workers", "n", "p50_ms", "p99_ms", "max_ms", "ops/sec")
	fmt.Println(strings.Repeat("-", 78))
}

// printSweepRow emits one stdout row.
func printSweepRow(scenario string, workers int, s latencyStats) {
	p50ms := float64(s.p50.Microseconds()) / 1000.0
	p99ms := float64(s.p99.Microseconds()) / 1000.0
	maxms := float64(s.maxv.Microseconds()) / 1000.0
	fmt.Printf("%-14s %-9d %-7d %-9.2f %-9.2f %-9.2f %-10.0f\n",
		scenario, workers, s.n, p50ms, p99ms, maxms, s.opsPerSec)
}

// hotSweepCSV opens an output CSV in outDir and writes the standard
// header. Caller must Close the returned file.
func hotSweepCSV(outDir, name string) (*os.File, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	p := filepath.Join(outDir, name)
	f, err := os.Create(p)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", p, err)
	}
	if _, err := fmt.Fprintln(f, "scenario,workers,iters,p50_ms,p90_ms,p99_ms,max_ms,ops_per_sec,errors"); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func writeSweepRow(f *os.File, scenario string, workers int, s latencyStats, errs int) {
	p50ms := float64(s.p50.Microseconds()) / 1000.0
	p90ms := float64(s.p90.Microseconds()) / 1000.0
	p99ms := float64(s.p99.Microseconds()) / 1000.0
	maxms := float64(s.maxv.Microseconds()) / 1000.0
	fmt.Fprintf(f, "%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.1f,%d\n",
		scenario, workers, s.n, p50ms, p90ms, p99ms, maxms, s.opsPerSec, errs)
}

// cmdHotLedgerConcurrencySweep runs a (workers × page-size) sweep against
// the hot ledger RocksDB. page-size=1 is a point lookup; larger values
// iterate that many consecutive ledgers via IterateLedgers. The store is
// opened once and shared across all workers.
func cmdHotLedgerConcurrencySweep() {
	fs := flag.NewFlagSet("hot-ledger-concurrency-sweep", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir (full 10k-ledger RocksDB)")
	chunkN := fs.Uint("chunk", 5000, "chunk ID seeded into hot-dir")
	workersCSV := fs.String("workers", "1,2,4,8,16,24,32,48,64", "comma-separated worker counts to sweep")
	pageSizesCSV := fs.String("page-sizes", "1,10,100,1000", "comma-separated --n values to sweep (1 = point lookup)")
	itersPerWorker := fs.Int("iters", 200, "iterations per worker per scenario")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	pageSizes, err := parseIntList(*pageSizesCSV)
	if err != nil {
		fatal(logger, "parse --page-sizes: %v", err)
	}

	chunkID := uint32(*chunkN)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	hot, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore: %v", err)
	}
	defer hot.Close()

	if _, err := hot.GetLedgerRaw(first); err != nil {
		fatal(logger, "hot store missing first seq %d: %v (seed-hot first?)", first, err)
	}
	if _, err := hot.GetLedgerRaw(last); err != nil {
		fatal(logger, "hot store missing last seq %d: %v", last, err)
	}
	span := last - first + 1

	summaryF, ferr := hotSweepCSV(*outDir, "hot-ledger-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("hot-ledger sweep workers=%v page_sizes=%v iters=%d chunk=%d span=[%d,%d]",
		workers, pageSizes, *itersPerWorker, chunkID, first, last)
	printSweepHeader("page_size")

	var rows []sweepRow
	for _, n := range pageSizes {
		if uint32(n) > span {
			logger.Warnf("page-size %d > chunk span %d; skipping", n, span)
			continue
		}
		startSpan := span - uint32(n) + 1
		nLocal := uint32(n)

		op := func(rng *rand.Rand) error {
			if nLocal == 1 {
				seq := first + rng.Uint32N(span)
				raw, gerr := hot.GetLedgerRaw(seq)
				if gerr != nil {
					return fmt.Errorf("GetLedgerRaw(%d): %w", seq, gerr)
				}
				if len(raw) == 0 {
					return fmt.Errorf("empty payload seq=%d", seq)
				}
				return nil
			}
			start := first + rng.Uint32N(startSpan)
			end := start + nLocal - 1
			seen := uint32(0)
			for entry, ierr := range hot.IterateLedgers(start, end) {
				if ierr != nil {
					return fmt.Errorf("iterate seq=%d: %w", entry.Seq, ierr)
				}
				if len(entry.Bytes) == 0 {
					return fmt.Errorf("empty payload seq=%d", entry.Seq)
				}
				seen++
			}
			if seen != nLocal {
				return fmt.Errorf("got %d ledgers, expected %d", seen, nLocal)
			}
			return nil
		}

		for _, w := range workers {
			res := runHotConcurrent(w, *itersPerWorker, *seed, op)
			scenario := fmt.Sprintf("n=%d", n)
			printSweepRow(scenario, w, res.stats)
			writeSweepRow(summaryF, scenario, w, res.stats, res.totalErrs)
			rows = append(rows, sweepRow{n, w,
				float64(res.stats.p50.Microseconds()) / 1000.0,
				float64(res.stats.p99.Microseconds()) / 1000.0,
				res.stats.opsPerSec,
			})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per page size):")
	reportSaturation(rows)
	logger.Infof("wrote %s", filepath.Join(*outDir, "hot-ledger-concurrency-sweep.csv"))
}

// cmdHotTxHashConcurrencySweep runs a worker sweep of end-to-end
// transaction-by-hash lookups (txhash hot Get → ledger hot Get → linear
// scan in the LCM) against the hot stores.
func cmdHotTxHashConcurrencySweep() {
	fs := flag.NewFlagSet("hot-tx-hash-concurrency-sweep", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	txHotDir := fs.String("txhash-hot", "/mnt/nvme/disk2/ledgers/txhash-hot", "txhash hot store dir")
	txColdBin := fs.String("txhash-cold-bin", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin",
		"cold txhash sorted .bin (used purely as the corpus of valid hashes)")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	workersCSV := fs.String("workers", "1,2,4,8,16,24,32,48,64", "worker counts to sweep")
	itersPerWorker := fs.Int("iters", 200, "iterations per worker per scenario")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}

	chunkID := uint32(*chunkN)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	corpus, err := loadCorpus(*txColdBin)
	if err != nil {
		fatal(logger, "loadCorpus: %v", err)
	}
	if len(corpus) == 0 {
		fatal(logger, "empty corpus — run seed-txhash-cold first")
	}
	logger.Infof("corpus: %d (hash, seq) pairs from %s", len(corpus), *txColdBin)

	txh, herr := txhash.NewHotStore(*txHotDir, logger)
	if herr != nil {
		fatal(logger, "txhash NewHotStore: %v", herr)
	}
	defer txh.Close()

	lh, lerr := ledger.NewHotStore(*hotDir, logger)
	if lerr != nil {
		fatal(logger, "ledger NewHotStore: %v", lerr)
	}
	defer lh.Close()

	op := func(rng *rand.Rand) error {
		ce := corpus[rng.IntN(len(corpus))]
		seq, gerr := txh.Get(ce.hash)
		if gerr != nil {
			return fmt.Errorf("txhash Get: %w", gerr)
		}
		if seq != ce.seq {
			return fmt.Errorf("seq mismatch: got %d, expected %d", seq, ce.seq)
		}
		if seq < first || seq > last {
			return fmt.Errorf("seq %d outside chunk window [%d,%d]", seq, first, last)
		}
		raw, rerr := lh.GetLedgerRaw(seq)
		if rerr != nil {
			return fmt.Errorf("ledger Get(%d): %w", seq, rerr)
		}
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
			return fmt.Errorf("UnmarshalBinary: %w", uerr)
		}
		nTx := lcm.CountTransactions()
		for i := 0; i < nTx; i++ {
			if lcm.TransactionHash(i) == ce.hash {
				_ = lcm.TransactionResultPair(i)
				return nil
			}
		}
		return fmt.Errorf("hash not found in ledger %d", seq)
	}

	summaryF, ferr := hotSweepCSV(*outDir, "hot-tx-hash-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("hot-tx-hash sweep workers=%v iters=%d corpus=%d",
		workers, *itersPerWorker, len(corpus))
	printSweepHeader("scenario")

	var rows []sweepRow
	for _, w := range workers {
		res := runHotConcurrent(w, *itersPerWorker, *seed, op)
		printSweepRow("tx-hash", w, res.stats)
		writeSweepRow(summaryF, "tx-hash", w, res.stats, res.totalErrs)
		rows = append(rows, sweepRow{0, w,
			float64(res.stats.p50.Microseconds()) / 1000.0,
			float64(res.stats.p99.Microseconds()) / 1000.0,
			res.stats.opsPerSec,
		})
	}
	fmt.Println("\nSaturation summary (peak workers for tx-hash):")
	reportSaturation(rows)
	logger.Infof("wrote %s", filepath.Join(*outDir, "hot-tx-hash-concurrency-sweep.csv"))
}

// cmdHotEventsConcurrencySweep runs a (scenario × workers) sweep against
// the hot eventstore. Scenarios: no-filter (random N-ledger range),
// contract (single Lookup), topic (single Lookup), both (intersect two
// Lookups). All scenarios end with FetchEvents of up to --max-fetch IDs.
func cmdHotEventsConcurrencySweep() {
	fs := flag.NewFlagSet("hot-events-concurrency-sweep", flag.ExitOnError)
	hotEvents := fs.String("hot-events-dir", "/mnt/nvme/disk2/ledgers/events-hot", "hot eventstore dir")
	corpusPath := fs.String("corpus", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus path")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	scenariosCSV := fs.String("scenarios", "no-filter,contract,topic,both", "scenarios to sweep")
	workersCSV := fs.String("workers", "1,2,4,8,16,24,32,48,64", "worker counts to sweep")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker per scenario")
	rangeLedgers := fs.Int("range-ledgers", 50, "ledgers per fetch in no-filter scenario")
	maxFetch := fs.Int("max-fetch", 1000, "cap on per-iter eventIDs fed to FetchEvents")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	scenarios := strings.Split(*scenariosCSV, ",")
	for i, s := range scenarios {
		scenarios[i] = strings.TrimSpace(s)
	}

	tc, err := loadTermCorpus(*corpusPath)
	if err != nil {
		fatal(logger, "load corpus: %v", err)
	}
	if len(tc.ContractIDs) == 0 && len(tc.Topic0) == 0 {
		fatal(logger, "corpus has no contract IDs or topic0 entries")
	}
	logger.Infof("corpus: contracts=%d topic0=%d topic1=%d", len(tc.ContractIDs), len(tc.Topic0), len(tc.Topic1))

	chunkID := chunk.ID(uint32(*chunkN))
	hot, oerr := eventstore.OpenHotStore(*hotEvents, chunkID, logger)
	if oerr != nil {
		fatal(logger, "OpenHotStore: %v", oerr)
	}
	defer hot.Close()

	offsets, oerr := hot.Offsets()
	if oerr != nil {
		fatal(logger, "Offsets: %v", oerr)
	}

	pickTerm := func(rng *rand.Rand, keys []string) (events.TermKey, error) {
		if len(keys) == 0 {
			return events.TermKey{}, errors.New("empty corpus slice")
		}
		raw, derr := hex.DecodeString(keys[rng.IntN(len(keys))])
		if derr != nil || len(raw) != 16 {
			return events.TermKey{}, fmt.Errorf("bad hex key: %w", derr)
		}
		var k events.TermKey
		copy(k[:], raw)
		return k, nil
	}

	ctx := context.Background()
	ledgerCount := tc.LastLedger - tc.FirstLedger + 1
	if uint32(*rangeLedgers) > ledgerCount {
		fatal(logger, "--range-ledgers (%d) > chunk size (%d)", *rangeLedgers, ledgerCount)
	}
	startWindow := ledgerCount - uint32(*rangeLedgers)

	makeOp := func(scenario string) hotWorkload {
		switch scenario {
		case "no-filter":
			return func(rng *rand.Rand) error {
				startLedger := tc.FirstLedger + rng.Uint32N(startWindow)
				endLedger := startLedger + uint32(*rangeLedgers) - 1
				firstID, _, ferr := offsets.EventIDs(startLedger)
				if ferr != nil {
					return fmt.Errorf("EventIDs start: %w", ferr)
				}
				_, lastID, lerr := offsets.EventIDs(endLedger)
				if lerr != nil {
					return fmt.Errorf("EventIDs end: %w", lerr)
				}
				if lastID <= firstID {
					return nil
				}
				count := int(lastID - firstID)
				if count > *maxFetch {
					count = *maxFetch
				}
				ids := make([]uint32, count)
				for i := range ids {
					ids[i] = firstID + uint32(i)
				}
				_, fferr := hot.FetchEvents(ctx, ids)
				return fferr
			}
		case "contract":
			return func(rng *rand.Rand) error {
				k, kerr := pickTerm(rng, tc.ContractIDs)
				if kerr != nil {
					return kerr
				}
				bm, lerr := hot.Lookup(k)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup: %w", lerr)
				}
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := hot.FetchEvents(ctx, ids)
				return fferr
			}
		case "topic":
			return func(rng *rand.Rand) error {
				k, kerr := pickTerm(rng, tc.Topic0)
				if kerr != nil {
					return kerr
				}
				bm, lerr := hot.Lookup(k)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup: %w", lerr)
				}
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := hot.FetchEvents(ctx, ids)
				return fferr
			}
		case "both":
			return func(rng *rand.Rand) error {
				kc, kerr := pickTerm(rng, tc.ContractIDs)
				if kerr != nil {
					return kerr
				}
				kt, kerr := pickTerm(rng, tc.Topic0)
				if kerr != nil {
					return kerr
				}
				bmC, lerr := hot.Lookup(kc)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup contract: %w", lerr)
				}
				bmT, lerr := hot.Lookup(kt)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup topic: %w", lerr)
				}
				bm := roaring.And(bmC, bmT)
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := hot.FetchEvents(ctx, ids)
				return fferr
			}
		default:
			return nil
		}
	}

	summaryF, ferr := hotSweepCSV(*outDir, "hot-events-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("hot-events sweep scenarios=%v workers=%v iters=%d",
		scenarios, workers, *itersPerWorker)
	printSweepHeader("scenario")

	type evtRow struct {
		scenario string
		workers  int
		p50, p99 float64
		ops      float64
	}
	var rows []evtRow
	for _, sc := range scenarios {
		op := makeOp(sc)
		if op == nil {
			logger.Warnf("unknown scenario %q; skipping", sc)
			continue
		}
		for _, w := range workers {
			res := runHotConcurrent(w, *itersPerWorker, *seed, op)
			printSweepRow(sc, w, res.stats)
			writeSweepRow(summaryF, sc, w, res.stats, res.totalErrs)
			rows = append(rows, evtRow{sc, w,
				float64(res.stats.p50.Microseconds()) / 1000.0,
				float64(res.stats.p99.Microseconds()) / 1000.0,
				res.stats.opsPerSec,
			})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per scenario):")
	byScenario := map[string][]evtRow{}
	for _, r := range rows {
		byScenario[r.scenario] = append(byScenario[r.scenario], r)
	}
	scs := make([]string, 0, len(byScenario))
	for k := range byScenario {
		scs = append(scs, k)
	}
	sort.Strings(scs)
	fmt.Printf("%-14s %-12s %-10s %-10s %-12s\n",
		"scenario", "peak_workers", "peak_ops/s", "p50@peak", "p99@peak")
	for _, sc := range scs {
		rs := byScenario[sc]
		best := rs[0]
		for _, r := range rs[1:] {
			if r.ops > best.ops {
				best = r
			}
		}
		fmt.Printf("%-14s %-12d %-10.0f %-10.2f %-12.2f\n",
			sc, best.workers, best.ops, best.p50, best.p99)
	}

	logger.Infof("wrote %s", filepath.Join(*outDir, "hot-events-concurrency-sweep.csv"))
}

// cmdHotTxPageConcurrencySweep runs a (workers × page-size) sweep of
// "page of N transactions starting at cursor (seq, txIdx)" on the hot
// ledger RocksDB. The chunk is preflighted once (tx counts per ledger)
// and shared across workers; each worker picks a random cursor and
// walks ledgers forward via GetLedgerRaw until the page is full.
func cmdHotTxPageConcurrencySweep() {
	fs := flag.NewFlagSet("hot-tx-page-concurrency-sweep", flag.ExitOnError)
	hotDir := fs.String("hot-dir", "/mnt/nvme/disk2/ledgers/hot-5000", "hot ledger store dir")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	workersCSV := fs.String("workers", "1,2,4,8,16,32", "worker counts to sweep")
	pageSizesCSV := fs.String("page-sizes", "5,20,50,200", "transactions per page")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker per scenario")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	pageSizes, err := parseIntList(*pageSizesCSV)
	if err != nil {
		fatal(logger, "parse --page-sizes: %v", err)
	}

	chunkID := uint32(*chunkN)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)

	hot, err := ledger.NewHotStore(*hotDir, logger)
	if err != nil {
		fatal(logger, "NewHotStore: %v", err)
	}
	defer hot.Close()
	if _, err := hot.GetLedgerRaw(first); err != nil {
		fatal(logger, "hot store missing first seq %d: %v", first, err)
	}

	type ledgerInfo struct {
		seq     uint32
		txCount int
	}
	logger.Infof("preflight: scanning chunk %d for tx counts...", chunkID)
	infos := make([]ledgerInfo, 0, ledgersPerChunk)
	totalTx := 0
	for entry, ierr := range hot.IterateLedgers(first, last) {
		if ierr != nil {
			fatal(logger, "preflight iterate: %v", ierr)
		}
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(entry.Bytes); uerr != nil {
			fatal(logger, "preflight unmarshal seq=%d: %v", entry.Seq, uerr)
		}
		ct := lcm.CountTransactions()
		infos = append(infos, ledgerInfo{seq: entry.Seq, txCount: ct})
		totalTx += ct
	}
	logger.Infof("preflight: %d ledgers, %d tx (avg %.1f/ledger)",
		len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	summaryF, ferr := hotSweepCSV(*outDir, "hot-tx-page-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("hot-tx-page sweep workers=%v page_sizes=%v iters=%d chunk=%d",
		workers, pageSizes, *itersPerWorker, chunkID)
	printSweepHeader("page_size")

	var rows []sweepRow
	for _, page := range pageSizes {
		if totalTx < page {
			logger.Warnf("page %d > totalTx %d; skipping", page, totalTx)
			continue
		}
		pageLocal := page

		walkPage := func(ledgerIdx, txIdx int) (int, error) {
			remaining := pageLocal
			got := 0
			for i := ledgerIdx; i < len(infos) && remaining > 0; i++ {
				raw, gerr := hot.GetLedgerRaw(infos[i].seq)
				if gerr != nil {
					return got, gerr
				}
				var lcm goxdr.LedgerCloseMeta
				if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
					return got, uerr
				}
				nTx := lcm.CountTransactions()
				startIdx := 0
				if i == ledgerIdx {
					startIdx = txIdx
				}
				for j := startIdx; j < nTx && remaining > 0; j++ {
					_ = lcm.TransactionHash(j)
					_ = lcm.TransactionResultPair(j)
					got++
					remaining--
				}
			}
			return got, nil
		}

		pick := func(rng *rand.Rand) (int, int) {
			for {
				i := rng.IntN(len(infos))
				if infos[i].txCount == 0 {
					continue
				}
				j := rng.IntN(infos[i].txCount)
				ahead := infos[i].txCount - j
				for k := i + 1; k < len(infos) && ahead < pageLocal; k++ {
					ahead += infos[k].txCount
				}
				if ahead >= pageLocal {
					return i, j
				}
			}
		}

		op := func(rng *rand.Rand) error {
			li, ti := pick(rng)
			got, werr := walkPage(li, ti)
			if werr != nil {
				return werr
			}
			if got != pageLocal {
				return fmt.Errorf("short read: got %d want %d", got, pageLocal)
			}
			return nil
		}

		for _, w := range workers {
			res := runHotConcurrent(w, *itersPerWorker, *seed, op)
			scenario := fmt.Sprintf("page=%d", page)
			printSweepRow(scenario, w, res.stats)
			writeSweepRow(summaryF, scenario, w, res.stats, res.totalErrs)
			rows = append(rows, sweepRow{page, w,
				float64(res.stats.p50.Microseconds()) / 1000.0,
				float64(res.stats.p99.Microseconds()) / 1000.0,
				res.stats.opsPerSec,
			})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per page size):")
	reportSaturation(rows)
	logger.Infof("wrote %s", filepath.Join(*outDir, "hot-tx-page-concurrency-sweep.csv"))
}

// cmdColdTxHashConcurrencySweep runs a worker sweep of end-to-end
// transaction-by-hash on the COLD path: shared mmap'd sorted .bin for
// hash → seq, then per-iter FADV_DONTNEED-evict the ledger pack and a
// fresh ColdStoreReader for the ledger fetch. Single-chunk-only (the
// corpus is one .bin), so all workers race the same pack file's
// pagecache — interpret cache hit/miss ratios accordingly.
func cmdColdTxHashConcurrencySweep() {
	fs := flag.NewFlagSet("cold-tx-hash-concurrency-sweep", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold ledger pack root")
	txColdBin := fs.String("txhash-cold-bin", "/mnt/nvme/disk2/ledgers/txhash-cold/00005000.bin",
		"sorted .bin (corpus + hash→seq lookup)")
	chunkN := fs.Uint("chunk", 5000, "chunk ID (must match the .bin)")
	workersCSV := fs.String("workers", "1,2,4,8,16,32", "worker counts to sweep")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workersList, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}

	chunkID := uint32(*chunkN)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	pack := packPath(*coldDir, chunkID)
	if _, err := os.Stat(pack); err != nil {
		fatal(logger, "cold pack missing: %s: %v", pack, err)
	}

	corpus, err := loadCorpus(*txColdBin)
	if err != nil {
		fatal(logger, "loadCorpus: %v", err)
	}
	if len(corpus) == 0 {
		fatal(logger, "empty corpus")
	}
	sb, err := openSortedBin(*txColdBin)
	if err != nil {
		fatal(logger, "openSortedBin: %v", err)
	}
	logger.Infof("corpus: %d (hash, seq) pairs; ledger pack=%s", len(corpus), pack)

	summaryF, ferr := hotSweepCSV(*outDir, "cold-tx-hash-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("cold-tx-hash sweep workers=%v iters=%d corpus=%d",
		workersList, *itersPerWorker, len(corpus))
	printSweepHeader("scenario")

	var rows []sweepRow
	for _, w := range workersList {
		type workerResult struct {
			durs []time.Duration
			errs int
		}
		results := make([]workerResult, w)
		var wg sync.WaitGroup
		wg.Add(w)
		tStart := time.Now()
		for wID := 0; wID < w; wID++ {
			go func(id int) {
				defer wg.Done()
				dec := zstd.NewDecompressor()
				rng := rand.New(rand.NewPCG(
					uint64(*seed)+uint64(id),
					uint64(*seed*7919)+uint64(id),
				))
				durs := make([]time.Duration, 0, *itersPerWorker)
				var errs int
				for i := 0; i < *itersPerWorker; i++ {
					ce := corpus[rng.IntN(len(corpus))]
					if ce.seq < first || ce.seq > last {
						errs++
						continue
					}
					if eerr := evictFile(pack); eerr != nil {
						errs++
						continue
					}
					t0 := time.Now()
					cr, oerr := ledger.NewColdStoreReader(pack, dec)
					if oerr != nil {
						errs++
						continue
					}
					seq, ok := sb.lookupSeq(ce.hash)
					if !ok || seq != ce.seq {
						_ = cr.Close()
						errs++
						continue
					}
					raw, rerr := cr.GetLedgerRaw(seq)
					if rerr != nil {
						_ = cr.Close()
						errs++
						continue
					}
					var lcm goxdr.LedgerCloseMeta
					if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
						_ = cr.Close()
						errs++
						continue
					}
					found := false
					nTx := lcm.CountTransactions()
					for j := 0; j < nTx; j++ {
						if lcm.TransactionHash(j) == ce.hash {
							_ = lcm.TransactionResultPair(j)
							found = true
							break
						}
					}
					d := time.Since(t0)
					_ = cr.Close()
					if !found {
						errs++
						continue
					}
					durs = append(durs, d)
				}
				results[id] = workerResult{durs: durs, errs: errs}
			}(wID)
		}
		wg.Wait()
		wall := time.Since(tStart)

		all := make([]time.Duration, 0, w**itersPerWorker)
		totalErrs := 0
		for _, r := range results {
			all = append(all, r.durs...)
			totalErrs += r.errs
		}
		s := computeStats(all)
		s.opsPerSec = float64(len(all)) / wall.Seconds()
		printSweepRow("tx-hash-cold", w, s)
		writeSweepRow(summaryF, "tx-hash-cold", w, s, totalErrs)
		rows = append(rows, sweepRow{0, w,
			float64(s.p50.Microseconds()) / 1000.0,
			float64(s.p99.Microseconds()) / 1000.0,
			s.opsPerSec,
		})
	}
	fmt.Println("\nSaturation summary (peak workers for cold tx-hash):")
	reportSaturation(rows)
	logger.Infof("wrote %s", filepath.Join(*outDir, "cold-tx-hash-concurrency-sweep.csv"))
}

// cmdColdTxPageConcurrencySweep runs a (workers × page-size) sweep of
// tx-page on the COLD ledger packfile. One shared ColdStoreReader is
// opened up-front and probed for tx counts; workers issue concurrent
// GetLedgerRaw calls against it. Matches the methodology used by the
// existing single-threaded "tx-page cold" measurement in summary.csv.
func cmdColdTxPageConcurrencySweep() {
	fs := flag.NewFlagSet("cold-tx-page-concurrency-sweep", flag.ExitOnError)
	coldDir := fs.String("cold-dir", "/mnt/nvme/disk2/ledgers/cold", "cold ledger pack root")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	workersCSV := fs.String("workers", "1,2,4,8,16,32", "worker counts to sweep")
	pageSizesCSV := fs.String("page-sizes", "5,20,50,200", "transactions per page")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker per scenario")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	pageSizes, err := parseIntList(*pageSizesCSV)
	if err != nil {
		fatal(logger, "parse --page-sizes: %v", err)
	}

	chunkID := uint32(*chunkN)
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	pack := packPath(*coldDir, chunkID)
	dec := zstd.NewDecompressor()
	cr, err := ledger.NewColdStoreReader(pack, dec)
	if err != nil {
		fatal(logger, "NewColdStoreReader: %v", err)
	}
	defer cr.Close()

	type ledgerInfo struct {
		seq     uint32
		txCount int
	}
	logger.Infof("preflight: scanning chunk %d for tx counts...", chunkID)
	infos := make([]ledgerInfo, 0, ledgersPerChunk)
	totalTx := 0
	for entry, ierr := range cr.IterateLedgers(first, last) {
		if ierr != nil {
			fatal(logger, "preflight iterate: %v", ierr)
		}
		var lcm goxdr.LedgerCloseMeta
		if uerr := lcm.UnmarshalBinary(entry.Bytes); uerr != nil {
			fatal(logger, "preflight unmarshal seq=%d: %v", entry.Seq, uerr)
		}
		ct := lcm.CountTransactions()
		infos = append(infos, ledgerInfo{seq: entry.Seq, txCount: ct})
		totalTx += ct
	}
	logger.Infof("preflight: %d ledgers, %d tx (avg %.1f/ledger)",
		len(infos), totalTx, float64(totalTx)/float64(len(infos)))

	summaryF, ferr := hotSweepCSV(*outDir, "cold-tx-page-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("cold-tx-page sweep workers=%v page_sizes=%v iters=%d chunk=%d",
		workers, pageSizes, *itersPerWorker, chunkID)
	printSweepHeader("page_size")

	var rows []sweepRow
	for _, page := range pageSizes {
		if totalTx < page {
			logger.Warnf("page %d > totalTx %d; skipping", page, totalTx)
			continue
		}
		pageLocal := page

		walkPage := func(ledgerIdx, txIdx int) (int, error) {
			remaining := pageLocal
			got := 0
			for i := ledgerIdx; i < len(infos) && remaining > 0; i++ {
				raw, gerr := cr.GetLedgerRaw(infos[i].seq)
				if gerr != nil {
					return got, gerr
				}
				var lcm goxdr.LedgerCloseMeta
				if uerr := lcm.UnmarshalBinary(raw); uerr != nil {
					return got, uerr
				}
				nTx := lcm.CountTransactions()
				startIdx := 0
				if i == ledgerIdx {
					startIdx = txIdx
				}
				for j := startIdx; j < nTx && remaining > 0; j++ {
					_ = lcm.TransactionHash(j)
					_ = lcm.TransactionResultPair(j)
					got++
					remaining--
				}
			}
			return got, nil
		}

		pick := func(rng *rand.Rand) (int, int) {
			for {
				i := rng.IntN(len(infos))
				if infos[i].txCount == 0 {
					continue
				}
				j := rng.IntN(infos[i].txCount)
				ahead := infos[i].txCount - j
				for k := i + 1; k < len(infos) && ahead < pageLocal; k++ {
					ahead += infos[k].txCount
				}
				if ahead >= pageLocal {
					return i, j
				}
			}
		}

		op := func(rng *rand.Rand) error {
			li, ti := pick(rng)
			got, werr := walkPage(li, ti)
			if werr != nil {
				return werr
			}
			if got != pageLocal {
				return fmt.Errorf("short read: got %d want %d", got, pageLocal)
			}
			return nil
		}

		for _, w := range workers {
			res := runHotConcurrent(w, *itersPerWorker, *seed, op)
			scenario := fmt.Sprintf("page=%d", page)
			printSweepRow(scenario, w, res.stats)
			writeSweepRow(summaryF, scenario, w, res.stats, res.totalErrs)
			rows = append(rows, sweepRow{page, w,
				float64(res.stats.p50.Microseconds()) / 1000.0,
				float64(res.stats.p99.Microseconds()) / 1000.0,
				res.stats.opsPerSec,
			})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per page size):")
	reportSaturation(rows)
	logger.Infof("wrote %s", filepath.Join(*outDir, "cold-tx-page-concurrency-sweep.csv"))
}

// cmdColdEventsConcurrencySweep runs a (scenario × workers) sweep
// against a COLD events reader. One shared ColdReader is opened up-front;
// all workers share it and issue concurrent Lookup + FetchEvents calls.
// Matches the methodology used by the existing single-threaded "events
// cold" measurements in summary.csv.
func cmdColdEventsConcurrencySweep() {
	fs := flag.NewFlagSet("cold-events-concurrency-sweep", flag.ExitOnError)
	coldEvents := fs.String("cold-events-dir", "/mnt/nvme/disk2/ledgers/events-cold", "cold eventstore bucket dir")
	corpusPath := fs.String("corpus", "/mnt/nvme/disk2/ledgers/events-corpus.json", "term corpus path")
	chunkN := fs.Uint("chunk", 5000, "chunk ID")
	scenariosCSV := fs.String("scenarios", "no-filter,contract,topic,both", "scenarios to sweep")
	workersCSV := fs.String("workers", "1,2,4,8,16,32", "worker counts to sweep")
	itersPerWorker := fs.Int("iters", 100, "iterations per worker per scenario")
	rangeLedgers := fs.Int("range-ledgers", 50, "ledgers per fetch in no-filter scenario")
	maxFetch := fs.Int("max-fetch", 1000, "cap on per-iter eventIDs fed to FetchEvents")
	seed := fs.Int64("seed", 1, "RNG seed")
	outDir := fs.String("out", "bench-out", "CSV output dir")
	_ = fs.Parse(os.Args[1:])

	logger := supportlog.New()
	logger.SetLevel(logrus.InfoLevel)

	workers, err := parseIntList(*workersCSV)
	if err != nil {
		fatal(logger, "parse --workers: %v", err)
	}
	scenarios := strings.Split(*scenariosCSV, ",")
	for i, s := range scenarios {
		scenarios[i] = strings.TrimSpace(s)
	}

	tc, err := loadTermCorpus(*corpusPath)
	if err != nil {
		fatal(logger, "load corpus: %v", err)
	}
	logger.Infof("corpus: contracts=%d topic0=%d topic1=%d", len(tc.ContractIDs), len(tc.Topic0), len(tc.Topic1))

	chunkID := chunk.ID(uint32(*chunkN))
	cr, oerr := eventstore.OpenColdReader(chunkID, *coldEvents, eventstore.ColdReaderOptions{})
	if oerr != nil {
		fatal(logger, "OpenColdReader: %v", oerr)
	}
	defer cr.Close()

	offsets, oerr := cr.Offsets()
	if oerr != nil {
		fatal(logger, "Offsets: %v", oerr)
	}

	pickTerm := func(rng *rand.Rand, keys []string) (events.TermKey, error) {
		if len(keys) == 0 {
			return events.TermKey{}, errors.New("empty corpus slice")
		}
		raw, derr := hex.DecodeString(keys[rng.IntN(len(keys))])
		if derr != nil || len(raw) != 16 {
			return events.TermKey{}, fmt.Errorf("bad hex key: %w", derr)
		}
		var k events.TermKey
		copy(k[:], raw)
		return k, nil
	}

	ctx := context.Background()
	ledgerCount := tc.LastLedger - tc.FirstLedger + 1
	if uint32(*rangeLedgers) > ledgerCount {
		fatal(logger, "--range-ledgers (%d) > chunk size (%d)", *rangeLedgers, ledgerCount)
	}
	startWindow := ledgerCount - uint32(*rangeLedgers)

	makeOp := func(scenario string) hotWorkload {
		switch scenario {
		case "no-filter":
			return func(rng *rand.Rand) error {
				startLedger := tc.FirstLedger + rng.Uint32N(startWindow)
				endLedger := startLedger + uint32(*rangeLedgers) - 1
				firstID, _, ferr := offsets.EventIDs(startLedger)
				if ferr != nil {
					return fmt.Errorf("EventIDs start: %w", ferr)
				}
				_, lastID, lerr := offsets.EventIDs(endLedger)
				if lerr != nil {
					return fmt.Errorf("EventIDs end: %w", lerr)
				}
				if lastID <= firstID {
					return nil
				}
				count := int(lastID - firstID)
				if count > *maxFetch {
					count = *maxFetch
				}
				ids := make([]uint32, count)
				for i := range ids {
					ids[i] = firstID + uint32(i)
				}
				_, fferr := cr.FetchEvents(ctx, ids)
				return fferr
			}
		case "contract":
			return func(rng *rand.Rand) error {
				k, kerr := pickTerm(rng, tc.ContractIDs)
				if kerr != nil {
					return kerr
				}
				bm, lerr := cr.Lookup(k)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup: %w", lerr)
				}
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := cr.FetchEvents(ctx, ids)
				return fferr
			}
		case "topic":
			return func(rng *rand.Rand) error {
				k, kerr := pickTerm(rng, tc.Topic0)
				if kerr != nil {
					return kerr
				}
				bm, lerr := cr.Lookup(k)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup: %w", lerr)
				}
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := cr.FetchEvents(ctx, ids)
				return fferr
			}
		case "both":
			return func(rng *rand.Rand) error {
				kc, kerr := pickTerm(rng, tc.ContractIDs)
				if kerr != nil {
					return kerr
				}
				kt, kerr := pickTerm(rng, tc.Topic0)
				if kerr != nil {
					return kerr
				}
				bmC, lerr := cr.Lookup(kc)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup contract: %w", lerr)
				}
				bmT, lerr := cr.Lookup(kt)
				if lerr != nil {
					if errors.Is(lerr, eventstore.ErrTermNotFound) {
						return nil
					}
					return fmt.Errorf("Lookup topic: %w", lerr)
				}
				bm := roaring.And(bmC, bmT)
				ids := bitmapToSortedIDs(bm, *maxFetch)
				if len(ids) == 0 {
					return nil
				}
				_, fferr := cr.FetchEvents(ctx, ids)
				return fferr
			}
		default:
			return nil
		}
	}

	summaryF, ferr := hotSweepCSV(*outDir, "cold-events-concurrency-sweep.csv")
	if ferr != nil {
		fatal(logger, "create CSV: %v", ferr)
	}
	defer summaryF.Close()

	logger.Infof("cold-events sweep scenarios=%v workers=%v iters=%d",
		scenarios, workers, *itersPerWorker)
	printSweepHeader("scenario")

	type evtRow struct {
		scenario string
		workers  int
		p50, p99 float64
		ops      float64
	}
	var rows []evtRow
	for _, sc := range scenarios {
		op := makeOp(sc)
		if op == nil {
			logger.Warnf("unknown scenario %q; skipping", sc)
			continue
		}
		for _, w := range workers {
			res := runHotConcurrent(w, *itersPerWorker, *seed, op)
			printSweepRow(sc, w, res.stats)
			writeSweepRow(summaryF, sc, w, res.stats, res.totalErrs)
			rows = append(rows, evtRow{sc, w,
				float64(res.stats.p50.Microseconds()) / 1000.0,
				float64(res.stats.p99.Microseconds()) / 1000.0,
				res.stats.opsPerSec,
			})
		}
		fmt.Println()
	}

	fmt.Println("\nSaturation summary (highest ops/sec per scenario):")
	byScenario := map[string][]evtRow{}
	for _, r := range rows {
		byScenario[r.scenario] = append(byScenario[r.scenario], r)
	}
	scs := make([]string, 0, len(byScenario))
	for k := range byScenario {
		scs = append(scs, k)
	}
	sort.Strings(scs)
	fmt.Printf("%-14s %-12s %-10s %-10s %-12s\n",
		"scenario", "peak_workers", "peak_ops/s", "p50@peak", "p99@peak")
	for _, sc := range scs {
		rs := byScenario[sc]
		best := rs[0]
		for _, r := range rs[1:] {
			if r.ops > best.ops {
				best = r
			}
		}
		fmt.Printf("%-14s %-12d %-10.0f %-10.2f %-12.2f\n",
			sc, best.workers, best.ops, best.p50, best.p99)
	}

	logger.Infof("wrote %s", filepath.Join(*outDir, "cold-events-concurrency-sweep.csv"))
}
