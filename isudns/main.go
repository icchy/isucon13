package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/miekg/dns"
)

const (
	powerDNSSubdomainAddressEnvKey = "ISUCON13_POWERDNS_SUBDOMAIN_ADDRESS"
	powerDNSZonePathEnvKey         = "ISUCON13_POWERDNS_ZONE_PATH"
)

var (
	powerDNSSubdomainAddress string
	powerDNSZonePath         string
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
		log.Printf("Query for %s (type: %s)\n", q.Name, dns.TypeToString[q.Qtype])
		switch q.Qtype {
		case dns.TypeSOA:
			rr, err := dns.NewRR(fmt.Sprintf("%s SOA %s %s 0 10800 3600 604800 3600", q.Name, "ns1 hostmaster.u.isucon.dev.", "isucon.isucon.net."))
			if err != nil {
				log.Printf("Failed to create SOA record: %s\n", err.Error())
				continue
			}
			m.Answer = append(m.Answer, rr)
		case dns.TypeNS:
			rr, err := dns.NewRR(fmt.Sprintf("%s NS %s", q.Name, "ns1.u.isucon.dev."))
			if err != nil {
				log.Printf("Failed to create NS record: %s\n", err.Error())
				continue
			}
			m.Answer = append(m.Answer, rr)
		case dns.TypeA:
			log.Printf("Query for %s\n", q.Name)
			_, ok := records.Load(q.Name)
			if ok {
				rr, err := dns.NewRR(fmt.Sprintf("%s A %s", q.Name, powerDNSSubdomainAddress))
				if err == nil {
					m.Answer = append(m.Answer, rr)
				}
			} else {
				rr, err := dns.NewRR(fmt.Sprintf("%s A", q.Name))
				if err != nil {
					log.Printf("Failed to create NXDOMAIN record: %s\n", err.Error())
					continue
				}
				m.Rcode = dns.RcodeNameError
				m.Answer = append(m.Answer, rr)
				continue
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

func loadZoneFile(zoneFilePath string) error {
	// example
	// ns1      0 IN A  <ISUCON_SUBDOMAIN_ADDRESS>
	//pipe     0 IN A  <ISUCON_SUBDOMAIN_ADDRESS>
	//test001  0 IN A  <ISUCON_SUBDOMAIN_ADDRESS>

	f, err := os.Open(zoneFilePath)
	if err != nil {
		return fmt.Errorf("failed to open zone file: %w", err)
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("failed to read zone file: %w", err)
	}

	for _, line := range strings.Split(string(body), "\n") {
		if line == "" {
			continue
		}
		if strings.Contains(line, "0 IN A") {
			s := strings.Split(line, " ")
			//if len(s) != 5 {
			//	fmt.Println(len(s))
			//	return fmt.Errorf("invalid zone file format (line: %s)", line)
			//}
			fmt.Println()
			records.Store(fmt.Sprintf("%s.u.isucon.dev.", s[0]), powerDNSSubdomainAddress)
		}
	}

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

	zonePath, ok := os.LookupEnv(powerDNSZonePathEnvKey)
	if !ok {
		log.Fatalf("environ %s must be provided", powerDNSZonePathEnvKey)
	}
	powerDNSZonePath = zonePath
	if err := loadZoneFile(powerDNSZonePath); err != nil {
		log.Fatalf("failed to load zone file: %s", err.Error())
	}

	db, err := connectDB()
	if err != nil {
		log.Fatalf("Failed to connect DB: %s\n", err.Error())
	}
	defer db.Close()
	dbConn = db

	eg := errgroup.Group{}

	eg.Go(func() error {
		port := 53
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
