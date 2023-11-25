package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/miekg/dns"
)

const (
	powerDNSSubdomainAddressEnvKey = "ISUCON13_POWERDNS_SUBDOMAIN_ADDRESS"
)

var (
	powerDNSSubdomainAddress string
	dbConn                   *sqlx.DB
)

var (
	records = sync.Map{}
)

//
//var records = map[string]string{
//	"test.u.isucon.dev.": "192.168.0.2",
//}

func connectDB() (*sqlx.DB, error) {
	const (
		networkTypeEnvKey = "ISUCON13_MYSQL_DIALCONFIG_NET"
		addrEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_ADDRESS"
		portEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_PORT"
		userEnvKey        = "ISUCON13_MYSQL_DIALCONFIG_USER"
		passwordEnvKey    = "ISUCON13_MYSQL_DIALCONFIG_PASSWORD"
		dbNameEnvKey      = "ISUCON13_MYSQL_DIALCONFIG_DATABASE"
		parseTimeEnvKey   = "ISUCON13_MYSQL_DIALCONFIG_PARSETIME"
	)

	conf := mysql.NewConfig()

	// 環境変数がセットされていなかった場合でも一旦動かせるように、デフォルト値を入れておく
	// この挙動を変更して、エラーを出すようにしてもいいかもしれない
	conf.Net = "tcp"
	conf.Addr = net.JoinHostPort("127.0.0.1", "3306")
	conf.User = "isucon"
	conf.Passwd = "isucon"
	conf.DBName = "isupipe"
	conf.ParseTime = true

	if v, ok := os.LookupEnv(networkTypeEnvKey); ok {
		conf.Net = v
	}
	if addr, ok := os.LookupEnv(addrEnvKey); ok {
		if port, ok2 := os.LookupEnv(portEnvKey); ok2 {
			conf.Addr = net.JoinHostPort(addr, port)
		} else {
			conf.Addr = net.JoinHostPort(addr, "3306")
		}
	}
	if v, ok := os.LookupEnv(userEnvKey); ok {
		conf.User = v
	}
	if v, ok := os.LookupEnv(passwordEnvKey); ok {
		conf.Passwd = v
	}
	if v, ok := os.LookupEnv(dbNameEnvKey); ok {
		conf.DBName = v
	}
	if v, ok := os.LookupEnv(parseTimeEnvKey); ok {
		parseTime, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("failed to parse environment variable '%s' as bool: %+v", parseTimeEnvKey, err)
		}
		conf.ParseTime = parseTime
	}

	db, err := sqlx.Open("mysql", conf.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func parseQuery(m *dns.Msg, db sqlx.DB) {
	for _, q := range m.Question {
		switch q.Qtype {
		case dns.TypeA:
			log.Printf("Query for %s\n", q.Name)
			//username := q.Name[:len(q.Name)-len(".u.isucon.dev.")]
			//ip := records[q.Name]
			//records.Range(func(key, value interface{}) bool {
			//	log.Printf("key: %s, value: %s\n", key, value)
			//	return true
			//})
			_, ok := records.Load(q.Name)
			if ok {
				rr, err := dns.NewRR(fmt.Sprintf("%s A %s", q.Name, powerDNSSubdomainAddress))
				if err == nil {
					m.Answer = append(m.Answer, rr)
				}
			} else {
				// not found
				//var user User
				//err := db.Get(&user, "SELECT name FROM users WHERE name = ?", username)
				//// check no rows
				//if errors.Is(err, sql.ErrNoRows) {
				//	// not found

				rr, err := dns.NewRR(fmt.Sprintf("%s A", q.Name))
				if err != nil {
					log.Printf("Failed to create NXDOMAIN record: %s\n", err.Error())
					continue
				}
				m.Rcode = dns.RcodeNameError
				m.Answer = append(m.Answer, rr)
				continue
				//}
				//if err != nil && !errors.Is(err, sql.ErrNoRows) {
				//	log.Printf("Failed to get record from DB: %s\n", err.Error())
				//	continue
				//}
				//
				//// found
				//rr, err := dns.NewRR(fmt.Sprintf("%s A %s", q.Name, powerDNSSubdomainAddress))
				//if err == nil {
				//	m.Answer = append(m.Answer, rr)
				//	records.Store(q.Name, powerDNSSubdomainAddress)
				//	continue
				//}
				//log.Printf("found record but got error (name: %s): %s\n", q.Name, err)
			}
		}
	}
}

func handleDnsRequest(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Compress = false

	switch r.Opcode {
	case dns.OpcodeQuery:
		parseQuery(m, *dbConn)
	}

	w.WriteMsg(m)
}

type RecordCreateParam struct {
	Username string `json:"username"`
}

func HandleAddRecord(w http.ResponseWriter, r *http.Request) error {
	// TODO
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("method not allowed"))
		return fmt.Errorf("method not allowed")
	}

	param := RecordCreateParam{}
	if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("failed to decode request body"))
		return fmt.Errorf("failed to decode request body: %w", err)
	}

	records.Store(fmt.Sprintf("%s.u.isucon.dev.", param.Username), powerDNSSubdomainAddress)
	w.WriteHeader(http.StatusCreated)
	log.Printf("Created record for %s\n", param.Username)
	return nil
}

func main() {
	// attach request handler func
	dns.HandleFunc("u.isucon.dev.", handleDnsRequest)

	subdomainAddr, ok := os.LookupEnv(powerDNSSubdomainAddressEnvKey)
	if !ok {
		log.Fatalf("environ %s must be provided", powerDNSSubdomainAddressEnvKey)
	}
	powerDNSSubdomainAddress = subdomainAddr

	db, err := connectDB()
	if err != nil {
		log.Fatalf("Failed to connect DB: %s\n", err.Error())
	}
	defer db.Close()
	dbConn = db

	eg := errgroup.Group{}

	eg.Go(func() error {
		port := 5353
		server := &dns.Server{Addr: ":" + strconv.Itoa(port), Net: "udp"}
		log.Printf("Starting at %d\n", port)
		err = server.ListenAndServe()
		defer server.Shutdown()
		if err != nil {
			log.Fatalf("Failed to start server: %s\n ", err.Error())
		}
		return nil
	})

	eg.Go(func() error {
		// start http server
		http.HandleFunc("/api/record", func(w http.ResponseWriter, r *http.Request) {
			if err := HandleAddRecord(w, r); err != nil {
				log.Printf("Failed to handle request: %s\n", err.Error())
			}
		})
		port := 8082
		log.Printf("Starting at %d\n", port)
		err = http.ListenAndServe(":"+strconv.Itoa(port), nil)
		if err != nil {
			log.Fatalf("Failed to start server: %s\n ", err.Error())
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatalf("Failed to start server: %s\n ", err.Error())
	}
}
