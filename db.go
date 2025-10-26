// Copyright © by Jeff Foley 2017-2025. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// SPDX-License-Identifier: Apache-2.0

package assetdb

import (
	"context"
	"crypto/tls"
	"embed"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	neomigrations "github.com/garthoid/asset-db/migrations/neo4j"
	pgmigrations "github.com/garthoid/asset-db/migrations/postgres"
	sqlitemigrations "github.com/garthoid/asset-db/migrations/sqlite3"
	"github.com/garthoid/asset-db/repository"
	"github.com/garthoid/asset-db/repository/neo4j"
	"github.com/garthoid/asset-db/repository/sqlrepo"
	"github.com/glebarez/sqlite"
	neo4jdb "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	migrate "github.com/rubenv/sql-migrate"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// New creates a new assetDB instance.
// It initializes the asset database with the specified database type and DSN.
func New(dbtype, dsn string) (repository.Repository, error) {
	if dbtype == sqlrepo.SQLiteMemory {
		dsn = fmt.Sprintf("file:mem%d?mode=memory&cache=shared", rand.Intn(1000))
	}

	db, err := repository.New(dbtype, dsn)
	if err != nil {
		return nil, err
	}
	if err := migrateDatabase(dbtype, dsn); err != nil {
		return nil, err
	}
	return db, nil
}

func migrateDatabase(dbtype, dsn string) error {
	switch dbtype {
	case sqlrepo.SQLite:
		fallthrough
	case sqlrepo.SQLiteMemory:
		return sqlMigrate("sqlite3", sqlite.Open(dsn), sqlitemigrations.Migrations())
	case sqlrepo.Postgres:
		return sqlMigrate("postgres", postgres.Open(dsn), pgmigrations.Migrations())
	case neo4j.Neo4j:
		return neoMigrate(dsn)
	}
	return nil
}

func sqlMigrate(name string, database gorm.Dialector, fs embed.FS) error {
	sql, err := gorm.Open(database, &gorm.Config{})
	if err != nil {
		return err
	}

	migrationsSource := migrate.EmbedFileSystemMigrationSource{
		FileSystem: fs,
		Root:       "/",
	}

	sqlDb, err := sql.DB()
	if err != nil {
		return err
	}
	defer func() { _ = sqlDb.Close() }()

	_, err = migrate.Exec(sqlDb, name, migrationsSource, migrate.Up)
	if err != nil {
		return err
	}
	return nil
}

func neoMigrate(dsn string) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}

	auth := neo4jdb.NoAuth()
	var username, password string
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
		auth = neo4jdb.BasicAuth(username, password, "")
	}
	dbname := strings.TrimPrefix(u.Path, "/")

	// --- SUGGESTED CHANGE: START ---
	// Use the original DSN. The driver natively handles bolt+s and bolt+ssc.
	originalDSN := dsn
	var tlsConfig *tls.Config // Will remain nil for +s and +ssc

	switch u.Scheme {
	case "bolt+ssc", "neo4j+ssc":
		// Let the driver handle this scheme natively
	case "bolt+s", "neo4j+s":
		// Let the driver handle this scheme natively
	case "bolt", "neo4j":
		// Driver may default to encryption, so explicitly disable it.
		tlsConfig = nil
	default:
		return fmt.Errorf("neoMigrate: unsupported scheme %q", u.Scheme)
	}
	// --- SUGGESTED CHANGE: END ---

	driver, err := neo4jdb.NewDriverWithContext(originalDSN, auth, func(cfg *config.Config) { // <-- Use originalDSN
		cfg.MaxConnectionPoolSize = 20
		cfg.MaxConnectionLifetime = time.Hour
		cfg.ConnectionLivenessCheckTimeout = 10 * time.Minute
		// --- SUGGESTED CHANGE: START ---
		// Only set TlsConfig if we're *forcing* no-TLS.
		if u.Scheme == "bolt" || u.Scheme == "neo4j" {
			cfg.TlsConfig = tlsConfig // which is nil
		}
		// --- SUGGESTED CHANGE: END ---
	})
	if err != nil {
		return fmt.Errorf("neoMigrate: create driver: %w", err)
	}

	// Set timeout for TLS Handshake and initial connect.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := driver.VerifyConnectivity(ctx); err != nil {
		// --- SUGGESTED CHANGE: Use originalDSN in error ---
		return fmt.Errorf("neoMigrate: verify connectivity to %s: %w", originalDSN, err)
	}

	defer func() { _ = driver.Close(context.Background()) }()

	return neomigrations.InitializeSchema(driver, dbname)
}
