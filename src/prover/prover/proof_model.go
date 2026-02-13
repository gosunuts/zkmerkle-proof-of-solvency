package prover

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/binance/zkmerkle-proof-of-solvency/src/utils"
)

const (
	TableNamePrefix = "proof"
)

type (
	ProofModel interface {
		CreateProofTable() error
		DropProofTable() error
		CreateProof(row *Proof) error
		GetProofsBetween(start int64, end int64) (proofs []*Proof, err error)
		GetLatestProof() (p *Proof, err error)
		GetLatestConfirmedProof() (p *Proof, err error)
		GetProofByBatchNumber(height int64) (p *Proof, err error)
		GetRowCounts() (count int64, err error)
	}

	defaultProofModel struct {
		table string
		db    *utils.DB
	}

	Proof struct {
		ID                      uint64
		CreatedAt               time.Time
		UpdatedAt               time.Time
		DeletedAt               *time.Time
		ProofInfo               string
		CexAssetListCommitments string
		AccountTreeRoots        string
		BatchCommitment         string
		AssetsCount             int
		BatchNumber             int64
	}
)

func (m *defaultProofModel) TableName() string {
	return m.table
}

func NewProofModel(db *utils.DB, suffix string) ProofModel {
	return &defaultProofModel{
		table: TableNamePrefix + suffix,
		db:    db,
	}
}

func (m *defaultProofModel) CreateProofTable() error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		deleted_at TIMESTAMP NULL DEFAULT NULL,
		proof_info LONGTEXT NOT NULL,
		cex_asset_list_commitments TEXT NOT NULL,
		account_tree_roots TEXT NOT NULL,
		batch_commitment TEXT NOT NULL,
		assets_count INT NOT NULL,
		batch_number BIGINT NOT NULL UNIQUE
	)`, m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultProofModel) DropProofTable() error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.table)
	_, err := m.db.Exec(query)
	return err
}

func (m *defaultProofModel) CreateProof(row *Proof) error {
	query := fmt.Sprintf("INSERT INTO %s (proof_info, cex_asset_list_commitments, account_tree_roots, batch_commitment, assets_count, batch_number, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())", m.table)
	result, err := m.db.Exec(query, row.ProofInfo, row.CexAssetListCommitments, row.AccountTreeRoots, row.BatchCommitment, row.AssetsCount, row.BatchNumber)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return utils.DbErrSqlOperation
	}
	return nil
}

func (m *defaultProofModel) GetProofsBetween(start int64, end int64) (proofs []*Proof, err error) {
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, proof_info, cex_asset_list_commitments, account_tree_roots, batch_commitment, assets_count, batch_number FROM %s WHERE batch_number >= ? AND batch_number <= ? AND deleted_at IS NULL ORDER BY batch_number", m.table)
	rows, err := m.db.QueryWithTimeout(query, start, end)
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		proof := &Proof{}
		err = rows.Scan(&proof.ID, &proof.CreatedAt, &proof.UpdatedAt, &proof.DeletedAt, &proof.ProofInfo, &proof.CexAssetListCommitments, &proof.AccountTreeRoots, &proof.BatchCommitment, &proof.AssetsCount, &proof.BatchNumber)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, proof)
	}

	if len(proofs) == 0 {
		return nil, utils.DbErrNotFound
	}
	return proofs, nil
}

func (m *defaultProofModel) GetLatestProof() (p *Proof, err error) {
	row := &Proof{}
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, proof_info, cex_asset_list_commitments, account_tree_roots, batch_commitment, assets_count, batch_number FROM %s WHERE deleted_at IS NULL ORDER BY batch_number DESC LIMIT 1", m.table)
	dbRow := m.db.QueryRowWithTimeout(query)
	err = dbRow.Scan(&row.ID, &row.CreatedAt, &row.UpdatedAt, &row.DeletedAt, &row.ProofInfo, &row.CexAssetListCommitments, &row.AccountTreeRoots, &row.BatchCommitment, &row.AssetsCount, &row.BatchNumber)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return row, nil
}

func (m *defaultProofModel) GetLatestConfirmedProof() (p *Proof, err error) {
	row := &Proof{}
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, proof_info, cex_asset_list_commitments, account_tree_roots, batch_commitment, assets_count, batch_number FROM %s WHERE deleted_at IS NULL ORDER BY batch_number DESC LIMIT 1", m.table)
	dbRow := m.db.QueryRowWithTimeout(query)
	err = dbRow.Scan(&row.ID, &row.CreatedAt, &row.UpdatedAt, &row.DeletedAt, &row.ProofInfo, &row.CexAssetListCommitments, &row.AccountTreeRoots, &row.BatchCommitment, &row.AssetsCount, &row.BatchNumber)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return row, nil
}

func (m *defaultProofModel) GetProofByBatchNumber(num int64) (p *Proof, err error) {
	row := &Proof{}
	query := fmt.Sprintf("SELECT id, created_at, updated_at, deleted_at, proof_info, cex_asset_list_commitments, account_tree_roots, batch_commitment, assets_count, batch_number FROM %s WHERE batch_number = ? AND deleted_at IS NULL LIMIT 1", m.table)
	dbRow := m.db.QueryRowWithTimeout(query, num)
	err = dbRow.Scan(&row.ID, &row.CreatedAt, &row.UpdatedAt, &row.DeletedAt, &row.ProofInfo, &row.CexAssetListCommitments, &row.AccountTreeRoots, &row.BatchCommitment, &row.AssetsCount, &row.BatchNumber)
	if err == sql.ErrNoRows {
		return nil, utils.DbErrNotFound
	}
	if err != nil {
		return nil, utils.ConvertMysqlErrToDbErr(err)
	}
	return row, nil
}

func (m *defaultProofModel) GetRowCounts() (count int64, err error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE deleted_at IS NULL", m.table)
	row := m.db.QueryRowWithTimeout(query)
	err = row.Scan(&count)
	if err != nil {
		return 0, utils.ConvertMysqlErrToDbErr(err)
	}
	return count, nil
}
