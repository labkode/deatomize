package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cs3org/reva/pkg/eosclient"
)

// status tells if a record is a nasty one and the reason why
type status uint32

func (s status) String() string {
	switch s {
	case notNasty:
		return "the_file_looks_good_and_could_be_repaired_because_we_might_a_valid_version"
	case repairable:
		return "the_file_can_be_automatically_repaired_because_we_have_found_a_valid_version"
	case nastyNotChunk:
		return "the_current-file_is_not_an_aborted_upload,_thus_we_do_not_know_if_it_is_a_good_file_or_not"
	case nastyNoVersions:
		return "there_are_no_versions_availabe_for_this_file"
	case nastyInvalidVersion:
		return "the_available_versions_are_invalid_because_they_are_chunked_uploads"
	case nastyNotExistsAnymore:
		return "the_current_file_does_not_exists_anymore"
	default:
		log.Fatal("invalid state")
		return "invalid record state"
	}
}

// reasons for being a nasty record
const (
	notNasty              = status(iota) // 0
	repairable                           // 1
	nastyNotChunk                        // 2
	nastyNoVersions                      // 3
	nastyInvalidVersion                  // 4
	nastyNotExistsAnymore                // 5
)

var (
	ctx    = context.Background()
	mgm    = flag.String("mgm", "root://eoshome.cern.ch", "mgm url")
	user   = flag.String("user", "root", "user rol to execute against MGM")
	group  = flag.String("group", "root", "group rol to execute against MGM")
	repair = flag.Bool("repair", false, "set to true to rollback a file from a valid version")
	// the expected format of the file is
	// mtime fxid
	// 1592324325 018edb0f
	// 1592324325 018edb0f
	// 1592324329 018edb40
	// 1592324329 018edb40
	// 1592324335 018edba3
	// 1592324335 018edba3
	// where the separator is whitespace
	file = flag.String("file", "./deatomize", "file containing files to deatomize")
)

type skipped struct {
	Recycle  int
	Versions int
	Atomic   int
}

func skipRecords(records []*record) (toret []*record, sk *skipped) {
	sk = &skipped{}
	client := getEosClient()
	for _, r := range records {
		fi, err := client.GetFileInfoByFXID(ctx, *user, *group, r.FXID)
		if err != nil {
			fmt.Printf("error getting md from EOS: %+v\n", err)
			continue
		}

		// check that file is under a nominal space, not under proc recycle or versions folder
		filename := fi.File
		if strings.Contains(filename, "/proc/recycle") {
			sk.Recycle++
			fmt.Printf("skip: file is in recycle: %+v\n", fi)
			continue

		} else if strings.Contains(filename, "sys.v") {
			sk.Versions++
			fmt.Printf("skip: file is a version: %+v\n", fi)
			continue

		} else if strings.Contains(filename, "sys.a") {
			sk.Atomic++
			fmt.Printf("skip: file is atomic: %+v\n", fi)
			continue
		}

		r.File = filename
		toret = append(toret, r)
	}
	return toret, sk
}

func main() {
	flag.Parse()

	// get the records from the CSV white-space separated file.
	records := getRecords(*file)

	// skip records that have been overwriten, are in trashbin or are versions.
	newRecords, sk := skipRecords(records)

	// get chunked records and nasty records
	// chunked: the size is a multiple of a owncloud chunk of 10MB
	// nasty: the size is NOT a multiple of a owncloud chunk of 10MB.
	chunked, nasty := getRecordDistribution(newRecords)

	// Fix for chunked:
	// the current file for the user is an aborted file which makes no sense.
	// So we are going to try to get the list of versions if they are available for this file
	// and take the latest one. If the latest has a size that is not a chunk, we revert back to it,
	// else we continue taking older versions until finding one that suits us.
	// If the are no versions of the available versions are not suited, the record moves to the nasty stack.
	// This programm runs in dry mode by default printing the plan to take for each record.
	count("Initial count for chunked records", chunked)
	count("Initial count for nasty records", nasty)

	chunked, nasty = analyze(chunked, nasty)

	fmt.Printf("total=%d to_analize=%d skip_recycle=%d skip_version=%d skip_atomic=%d\n",
		len(records), len(newRecords), sk.Recycle, sk.Versions, sk.Atomic)

	count("Automatic reparable records with valid version", chunked)
	count("Nasty records, need manual repair with backup/recycle", nasty)
	fmt.Println("Nasty record classification")
	countNasty(nasty, nastyInvalidVersion)
	countNasty(nasty, nastyNoVersions)
	countNasty(nasty, nastyNotChunk)
	countNasty(nasty, nastyNotExistsAnymore)

	analyzeNasty(nasty)
	printInvalid(nasty)

	rollback(chunked)

}

func analyzeNasty(nasty []*record) {
	client := getEosClient()
	for _, r := range nasty {
		r.Handled = true
		versions, err := client.ListVersions(ctx, *user, *group, r.File)
		if err != nil {
			log.Fatal(err)
		}

		r.Versions = versions // add versions to the record
	}
}

func printInvalid(nasty []*record) {
	for _, r := range nasty {
		fmt.Println(r)
	}
}
func countNasty(nasty []*record, s status) {
	var c uint32
	for _, r := range nasty {
		if r != nil && r.Status == s {
			c++
		}
	}
	fmt.Printf("%s: %d\n", s.String(), c)
}

func rollback(chunked []*record) {
	client := getEosClient()
	for i, r := range chunked {
		fmt.Printf("dry-run=%t rollback (%d/len(%d): file=%s version=%s\n", !*repair, i, len(chunked), r.File, path.Base(r.ValidVersion.File))
		if *repair {
			if err := client.RollbackToVersion(ctx, *user, *group, r.File, path.Base(r.ValidVersion.File)); err != nil {
				fmt.Printf("error rollbacking %s: %s\n", r, err)
			}
		}
	}
}

func analyze(chunked, nasty []*record) ([]*record, []*record) {
	var newchunked []*record
	client := getEosClient()
	// go over all chunked records and try and classify them
	for i, r := range chunked {
		fmt.Printf("analyzing chunked records (%d/len(%d): %s\n", i, len(chunked), r)
		versions, err := client.ListVersions(ctx, *user, *group, r.File)
		if err != nil {
			log.Fatal(err)
		}

		r.Versions = versions // add versions to the record
		if len(r.Versions) == 0 {
			r.Status = nastyNoVersions
			nasty = append(nasty, r)
		} else {
			if valid := haveValidVersion(r); valid {
				newchunked = append(newchunked, r)
			} else {
				r.Status = nastyInvalidVersion
				nasty = append(nasty, r)
			}
		}
	}
	return newchunked, nasty
}

func haveValidVersion(r *record) bool {
	sortVersions(r.Versions)
	for _, v := range r.Versions {
		if isChunked(v.Size) {
			continue
		}

		// not chunked
		// mark it as valid
		r.Status = notNasty
		r.ValidVersion = v
		return true
	}
	return false
}

// sort versions from newest to oldest based on mtime reported from Eos.
func sortVersions(versions []*eosclient.FileInfo) {
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].MTimeSec > versions[j].MTimeSec
	})
}

func getEosClient() *eosclient.Client {
	// we need to connect to EOS
	opts := &eosclient.Options{
		URL: *mgm,
	}

	// create eos client
	return eosclient.New(opts)
}

func count(msg string, records []*record) {
	var c uint32
	for _, r := range records {
		if r != nil {
			c++
		}
	}
	fmt.Printf("%s: %d\n", msg, c)
}

func printRecords(records []*record) {
	for _, r := range records {
		fmt.Printf("%d %s %s\n", r.Size, r.Date, r.File)
	}
}

func getRecords(file string) (records []*record) {
	// read file
	data, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(strings.NewReader(string(data)))
	r.Comma = ' ' // file is space separated
	for {
		each, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// validate that we get only  two fields, else abort
		if len(each) != 2 {
			log.Fatal(fmt.Sprintf("error: record is invalid: %s", err))
		}

		r := &record{FXID: strings.TrimSpace(each[1])}

		// parse date
		i, err := strconv.ParseInt(each[0], 10, 64)
		if err != nil {
			log.Fatal(fmt.Sprintf("error: record is invalid: %s", err))
		}

		t := time.Unix(i, 0)
		if err != nil {
			log.Fatal(fmt.Sprintf("error: record size is not an uint64: %s", err))
		}
		r.Date = t

		records = append(records, r)
	}
	return
}

func getRecordDistribution(records []*record) (chunked, nasty []*record) {
	for i, r := range records {
		if isChunked(r.Size) {
			chunked = append(chunked, r)
		} else {
			r.Status = nastyNotChunk
			nasty = append(nasty, r)
		}
		r.Handled = true
		fmt.Printf("processing records (%d/len(%d): %s\n", i, len(chunked), r)
	}
	return
}

// chunks is ownCloud are 10MB
func isChunked(size uint64) bool {
	if size%(10*1000000) == 0 { // 10MB {}
		return true
	}
	return false
}

// a record parses an input file like this one:
// 38371328          20191025       /eos/user/m/mdavis/CERNHome/.thunderbird/7njy9cjc.default/global-messages-db.sqlite
type record struct {
	FXID         string
	Handled      bool
	Size         uint64
	Date         time.Time
	File         string
	Status       status                // the status of the record, check top of the file for definition.
	Versions     []*eosclient.FileInfo // available versions for this record if any.
	ValidVersion *eosclient.FileInfo   // the version key of a valid version to rollback.
}

func (r *record) String() string {
	var validVersion, status string
	if r.ValidVersion != nil {
		validVersion = r.ValidVersion.File
	}
	if r.Status == 0 {
		status = "HEISENBERG"
	} else if r.Status == 1 {
		status = "GOOD"
	} else {
		status = "BAD"
	}

	if r.Handled {
		return fmt.Sprintf("status=%s reason=%s size=%d date=%s versions=%d current_file=%s valid_version=%s",
			status,
			r.Status.String(),
			r.Size,
			r.Date,
			len(r.Versions),
			r.File,
			validVersion,
		)
	}
	return fmt.Sprintf("size=%d date=%s current_file=%s",
		r.Size,
		r.Date,
		r.File,
	)

}
