package main

import (
  "fmt"
  "os"
  "bufio"
  "regexp"
  "strings"
  "flag"
  "encoding/json"
  "time"
  "strconv"
)

import (
  _ "github.com/lib/pq"
  "database/sql"
  "github.com/briandowns/spinner"
)

var configFileFlag = flag.String("config", "", "the config file")
var bucketName = flag.String("bucketName", "", "which bucket this match will go to")
var bucketFilter = flag.String("bucketFilter", "", "which regex will match this bucket")
var testIt = flag.Bool("testData", true, "wait for test data on stdin")
var eatIt = flag.Bool("eatIt", true, "on a match, don't pass the event on to other buckets")
var reportIt = flag.Bool("reportIt", true, "record matches")
var updateCounts = flag.Bool("updateCounts", true, "update filter usage count on matches")
var apply = flag.Bool("apply", true, "apply bucket to unclassified events")
type hostList []string
var hostListFlag hostList

type Configuration struct {
  DBConn []string
}

// some functions to handle comma-separated arg lists
func (list *hostList) String() string {
  return fmt.Sprint(*list)
}

func (list *hostList) Set(value string) error {
  for _, h := range strings.Split(value, ",") {
    *list = append(*list, h)
  }
  return nil
}

func init() {
  flag.Var(&hostListFlag, "onlyOn", "comma-separated list of hosts to restrict this filter to matching on")
}

func main() {
  var db *sql.DB
  
  flag.Parse()
  
  if len(os.Args) == 1 {
    flag.PrintDefaults()
    os.Exit(0)
  }

  if *configFileFlag == "" {
    fmt.Println("I need a config file!")
    os.Exit(1)
  } else {
    configFile, err := os.Open(*configFileFlag)
    if err != nil {
      fmt.Println("opening config file:", err)
      os.Exit(2)
    }
    
    decoder := json.NewDecoder(configFile)
    configuration := &Configuration{}
    decoder.Decode(&configuration)
    
    db, err = sql.Open("postgres", configuration.DBConn[0])
    if err != nil {
      fmt.Println("couldn't connect to db", err)
      os.Exit(2)
    }
  }
  
  if *bucketName == "" {
    fmt.Println("I need a bucket name!")
    os.Exit(1)
  } 
  
  scanner := bufio.NewScanner(os.Stdin)

  if *bucketFilter == "" {
    // take the filter from stdin
    fmt.Print("I need a bucket filter, so give me one now: ")
    scanner.Scan()
    line := scanner.Text()
    if err := scanner.Err(); err != nil {
      fmt.Println("couldn't read a filter from stdin", err)
      os.Exit(2)
    }
    bucketFilter = &line
  }

  // verify the bucket filter is valid regex
  re, err := regexp.Compile(*bucketFilter)
  if err != nil {
    fmt.Println("regex compile error for", *bucketFilter, err)
    os.Exit(2)
  }
  
  if *testIt {
    testThis := ""
    fmt.Println("Give me a test line to make sure the regex you gave me does what you want it to:")
    for scanner.Scan() {
      testThis = testThis + scanner.Text()
      if err := scanner.Err(); err != nil {
        fmt.Println("couldn't read a test line from stdin", err)
        os.Exit(2)
      }
    }

    if matched := re.MatchString(testThis); !matched {
      fmt.Println("Looks like your regex '" + *bucketFilter + "' doesn't work for your test case of '" + testThis + "'!")
      os.Exit(4)
    }
  }

  tx, err := db.Begin()
  if err != nil {
    fmt.Println("ruh-oh, couldn't start transaction to insert new bucket filter", err)
    os.Exit(3)
  }

  // Let's see if we already have this bucket.
  // If we do, then we should just add this filter to the existing bucket. 
  var id int32
  err = tx.QueryRow(`select id from buckets where name = $1`, *bucketName).Scan(&id)
  switch {
    default:
      fmt.Println("Adding to bucket called \"", *bucketName, "\"using filter \"", *bucketFilter, "\"...")
    case err == sql.ErrNoRows:
      // If we don't, then make a new one.
      err = tx.QueryRow(`INSERT INTO buckets(name, eat_it, report_it) VALUES($1, $2, $3) RETURNING id`, *bucketName, *eatIt, *reportIt).Scan(&id)
      if err != nil {
        fmt.Println("couldn't insert new bucket", err)
        os.Exit(3)
      }
      fmt.Println("Making a new bucket called \"", *bucketName, "\"using filter \"", *bucketFilter, "\"...")
    case err != nil:
        fmt.Println("couldn't search for existing bucket", err)
        os.Exit(3)
  }
  if len(hostListFlag) > 0 {
    fmt.Println(".... but only for", strings.Join(hostListFlag,", "))
  }
  _, err = tx.Exec(`INSERT INTO filters(bucket_id, filter, report) VALUES($1, $2, $3)`, id, *bucketFilter, *updateCounts)
  if err != nil {
    fmt.Println("couldn't insert new bucket filter", err)
    os.Exit(3)
  }
  if len(hostListFlag) > 0 {
    for h := 0; h < len(hostListFlag); h++ {
      _, err := tx.Exec(`INSERT INTO onlyon(bucket_id, host) VALUES($1, $2)`, id, hostListFlag[h])
      if err != nil {
        fmt.Println("couldn't insert new bucket host restriction", err)
        os.Exit(3)
      }
    }
  }
  
  if *apply {
    s := spinner.New(spinner.CharSets[9], 100*time.Millisecond) 
    s.Prefix = "Looking for unclassified events for this new filter: "
    s.Start()
    
    rows, err := db.Query(`select distinct event from events where bucket_id is null`)
    if err != nil {
      fmt.Println("couldn't find unclassified events", err)
      os.Exit(3)
    }

    s.Stop()
    fmt.Println("")
    s.Prefix = "Matching events: "
    s.Start()
    matches := 0
    matchables := 0

    for rows.Next() {
      var event string
      if err = rows.Scan(&event); err != nil {
        fmt.Println("couldn't read existing event", err)
        os.Exit(3)
      }

      matchables++

      // does this event match the new filter?
      if matched := re.MatchString(event); matched {
        matches++

        // let's update the bucket_ids for this event
        _, err := tx.Exec(`update events set bucket_id=$1 where bucket_id is null and event=$2`, id, event)
        if err != nil {
          fmt.Println("couldn't updated matching events", err)
          os.Exit(3)
        }
      }

      s.Suffix = " (filter matched " + strconv.Itoa(matches) + " of " + strconv.Itoa(matchables) + ")"
    }
    if err = rows.Err(); err != nil {
      fmt.Println("couldn't walk rows while looking for matching events", err)
      os.Exit(3)
    }
    
    rows.Close()

    s.Suffix = " (filter matched " + strconv.Itoa(matches) + " of " + strconv.Itoa(matchables) + ")"
    s.Stop()
  }

  err = tx.Commit()
  if err != nil {
    fmt.Println("ruh-oh, couldn't commit transaction to insert new bucket", err)
    os.Exit(3)
  }

  fmt.Println("   done!")
  
  db.Close();
}

