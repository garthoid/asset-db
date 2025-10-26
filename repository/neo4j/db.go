// Copyright © by Jeff Foley 2017-2025. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package neo4j

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"
	"time"

	neo4jdb "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

const Neo4j string = "neo4j"

// neoRepository is a repository implementation using Neo4j as the underlying DBMS.
type neoRepository struct {
	db     neo4jdb.DriverWithContext
	dbname string
}

// New creates a new instance of the asset database repository.
func New(dbtype, dsn string) (*neoRepository, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	auth := neo4jdb.NoAuth()
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
		auth = neo4jdb.BasicAuth(username, password, "")
	}
	dbname := strings.TrimPrefix(u.Path, "/")

	// --- FIX v2: START ---
	//
	// ALWAYS use the 'bolt://' scheme for the DSN.
	// The 'neo4j://' scheme is for cluster discovery and will fail.
	// We will manually configure TLS in the configFunc.
	newdsn := fmt.Sprintf("bolt://%s", u.Host)

	// The configFunc will manually configure TLS based on the *original* scheme
	configFunc := func(cfg *config.Config) {
		// Apply common settings
		cfg.MaxConnectionPoolSize = 20
		cfg.MaxConnectionLifetime = time.Hour
		cfg.ConnectionLivenessCheckTimeout = 10 * time.Minute

		switch u.Scheme {
		case "bolt+ssc", "neo4j+ssc":
			// Enable encryption AND skip verification
			cfg.TlsConfig = &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         u.Hostname(),
			}
		case "bolt+s", "neo4j+s":
			// Enable encryption AND perform full verification
			cfg.TlsConfig = &tls.Config{
				ServerName: u.Hostname(),
			}
		case "bolt", "neo4j":
			// Disable encryption
			cfg.TlsConfig = nil
		}
	}
	// --- FIX v2: END ---

	// Create driver with appropriate configuration
	driver, err := neo4jdb.NewDriverWithContext(newdsn, auth, configFunc)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(context.Background()) // best-effort cleanup to avoid leak
		return nil, err
	}

	return &neoRepository{db: driver, dbname: dbname}, nil
}

// Close implements the Repository interface.
func (neo *neoRepository) Close() error {
	return neo.db.Close(context.Background())
}

// GetDBType returns the type of the database.
func (neo *neoRepository) GetDBType() string {
	return Neo4j
}
