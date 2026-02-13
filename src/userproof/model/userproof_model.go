package model

import (
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/binance/zkmerkle-proof-of-solvency/src/utils"
)

const TableNamePreifx = "userproof"

type (
	UserProofModel interface {
		CreateUserProofTable() error
		DropUserProofTable() error
		CreateUserProofs(rows []UserProof) error
		GetUserProofByIndex(id uint32) (*UserProof, error)
		GetUserProofById(id string) (*UserProof, error)
		GetLatestAccountIndex() (uint32, error)
		GetUserCounts() (int, error)
	}

	defaultUserProofModel struct {
		table string
		db    *utils.DB
	}

	UserProof struct {
		AccountIndex    uint32
		AccountId       string
		AccountLeafHash string
		TotalEquity     string
		TotalDebt       string
		TotalCollateral string
		Assets          string
		Proof           string
		Config          string
		CreatedAt       time.Time
		UpdatedAt       time.Time
	}

	UserConfig struct {
		AccountIndex    uint32
		AccountIdHash   string
		TotalEquity     *big.Int
		TotalDebt       *big.Int
		TotalCollateral *big.Int
		Assets          []utils.AccountAsset
		Root            string
		Proof           [][]byte
	}
)

func (m *defaultUserProofModel) TableName() string {
	return m.table
}

func NewUserProofModel(db *utils.DB, suffix string) UserProofModel {
	return &defaultUserProofModel{
		table: TableNamePreifx + suffix,
		db:    db,
	}
}

func (m *defaultUserProofModel) CreateUserProofTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		account_index INT UNSIGNED NOT NULL UNIQUE,
		account_id VARCHAR(255) NOT NULL UNIQUE,
		account_leaf_hash TEXT NOT NULL,
		total_equity VARCHAR(255) NOT NULL,
		total_debt VARCHAR(255) NOT NULL,
		total_collateral VARCHAR(255) NOT NULL,
		assets LONGTEXT NOT NULL,
		proof LONGTEXT NOT NULL,
		config LONGTEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_int (account_index),
		INDEX idx_str (account_id)
	)`, m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultUserProofModel) DropUserProofTable() error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultUserProofModel) CreateUserProofs(rows []UserProof) error {
	if len(rows) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (account_index, account_id, account_leaf_hash, total_equity, total_debt, total_collateral, assets, proof, config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())", m.table)
	for _, row := range rows {
		_, err := m.db.Exec(query, row.AccountIndex, row.AccountId, row.AccountLeafHash, row.TotalEquity, row.TotalDebt, row.TotalCollateral, row.Assets, row.Proof, row.Config)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *defaultUserProofModel) GetUserProofByIndex(id uint32) (userproof *UserProof, err error) {
	userproof = &UserProof{}
	query := fmt.Sprintf("SELECT account_index, account_id, account_leaf_hash, total_equity, total_debt, total_collateral, assets, proof, config, created_at, updated_at FROM %s WHERE account_index = ? LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query, id)
	err = row.Scan(&userproof.AccountIndex, &userproof.AccountId, &userproof.AccountLeafHash, &userproof.TotalEquity, &userproof.TotalDebt, &userproof.TotalCollateral, &userproof.Assets, &userproof.Proof, &userproof.Config, &userproof.CreatedAt, &userproof.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return userproof, nil
}

func (m *defaultUserProofModel) GetUserProofById(id string) (userproof *UserProof, err error) {
	userproof = &UserProof{}
	query := fmt.Sprintf("SELECT account_index, account_id, account_leaf_hash, total_equity, total_debt, total_collateral, assets, proof, config, created_at, updated_at FROM %s WHERE account_id = ? LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query, id)
	err = row.Scan(&userproof.AccountIndex, &userproof.AccountId, &userproof.AccountLeafHash, &userproof.TotalEquity, &userproof.TotalDebt, &userproof.TotalCollateral, &userproof.Assets, &userproof.Proof, &userproof.Config, &userproof.CreatedAt, &userproof.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return userproof, nil
}

func (m *defaultUserProofModel) GetLatestAccountIndex() (uint32, error) {
	var index uint32
	query := fmt.Sprintf("SELECT account_index FROM %s ORDER BY account_index DESC LIMIT 1", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err := row.Scan(&index)
	if err == sql.ErrNoRows {
		return 0, utils.DbErrNotFound
	}
	if err != nil {
		return 0, utils.ConvertMysqlErrToDbErr(err)
	}
	return index, nil
}

func (m *defaultUserProofModel) GetUserCounts() (int, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err := row.Scan(&count)
	if err != nil {
		return 0, utils.ConvertMysqlErrToDbErr(err)
	}
	return int(count), nil
}
